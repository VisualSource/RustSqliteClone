use crate::sql::{interperter::ColumnData, Condition};

use super::{
    error::Error,
    node::Node,
    node_type::{NodeType, Schema},
    page::Page,
    pager::Pager,
    structure::{ConditionValue, Offset, Operation, Record, Value},
    wal::Wal,
};
use std::{path::PathBuf, vec};

pub struct BTree {
    pager: Pager,
    b: usize,
    wal: Wal,
}

pub struct BTreeBuilder {
    path: PathBuf,
    b: usize,
    offset: usize,
}

impl BTree {
    fn is_node_full(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            NodeType::Schema(_) => Ok(true),
            NodeType::Internal(_, keys) => Ok(keys.len() == (2 * self.b - 1)),
            NodeType::Leaf(rows) => Ok(rows.len() == (2 * self.b - 1)),
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }

    /*fn is_node_underflow(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            NodeType::Schema(_) => Ok(false),
            NodeType::Internal(_, keys) => Ok(keys.len() < self.b - 1 && !node.is_root),
            NodeType::Leaf(rows) => Ok(rows.len() < self.b - 1 && !node.is_root),
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }*/

    pub fn get_table(&mut self) -> Result<Schema, Error> {
        let page = self.pager.get_schema()?;

        let node = Node::try_from(page)?;

        match node.node_type {
            NodeType::Schema(schema) => Ok(schema),
            _ => Err(Error::Unexpected),
        }
    }

    pub fn select(
        &mut self,
        keep: &Vec<String>,
        target: &Option<Vec<Condition>>,
        _limit: Option<usize>,
    ) -> Result<Vec<Record>, Error> {
        let root_offset = self.wal.get_root()?;
        let root_page = self.pager.get_page(&root_offset)?;
        let root = Node::try_from(root_page)?;

        let schema = self.get_table()?;

        let selection = if let Some(cond) = target {
            Some(self.parse_conditions(&cond)?)
        } else {
            None
        };

        let indexs = if keep.len() == 0 {
            None
        } else {
            Some(schema.get_indexs_from_names(keep))
        };

        let mut results = vec![];

        self.select_node(root, &mut results, &indexs, &selection)?;

        Ok(results)
    }

    pub fn select_node(
        &mut self,
        node: Node,
        results: &mut Vec<Record>,
        indexs: &Option<Vec<usize>>,
        selection: &Option<Vec<ConditionValue>>,
    ) -> Result<(), Error> {
        match node.node_type {
            NodeType::Schema(_) => Err(Error::Unexpected),
            NodeType::Internal(offsets, _) => {
                for offset in offsets {
                    let page = self.pager.get_page(&offset)?;
                    let child_node = Node::try_from(page)?;
                    self.select_node(child_node, results, indexs, selection)?;
                }

                Ok(())
            }
            NodeType::Leaf(rows) => {
                if let Some(idex) = &indexs {
                    for row in rows {
                        if !row.match_condition(selection)? {
                            continue;
                        }
                        results.push(row.select_only(&idex))
                    }
                } else {
                    for row in rows {
                        if !row.match_condition(selection)? {
                            continue;
                        }
                        results.push(row);
                    }
                }

                Ok(())
            }
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }

    pub fn update(
        &mut self,
        columns: &Vec<(String, ColumnData)>,
        target: &Option<Vec<Condition>>,
    ) -> Result<(), Error> {
        let mut update = vec![];
        let schema = self.get_table()?;
        for (col, data) in columns {
            let item = match data {
                ColumnData::Null => (
                    Value::Null,
                    schema
                        .get_column_idx_by_name(&col)
                        .ok_or_else(|| Error::UnexpectedWithReason("Failed to get column."))?,
                ),
                ColumnData::Value(d) => schema.parse_value_by_col(&col, &d)?,
            };

            update.push(item);
        }

        let selection = if let Some(cond) = target {
            Some(self.parse_conditions(&cond)?)
        } else {
            None
        };

        let root_offset = self.wal.get_root()?;
        let root_page = self.pager.get_page(&root_offset)?;
        let mut root = Node::try_from(root_page)?;

        self.update_item(&update, &selection, &mut root, &root_offset)
    }

    fn update_item(
        &mut self,
        data: &Vec<(Value, usize)>,
        selection: &Option<Vec<ConditionValue>>,
        node: &mut Node,
        node_offset: &Offset,
    ) -> Result<(), Error> {
        match &mut node.node_type {
            NodeType::Schema(_) => {
                return Err(Error::UnexpectedWithReason(
                    "Should not update on schema node",
                ))
            }
            NodeType::Internal(children, _) => {
                for child_offset in children {
                    let child_page = self.pager.get_page(&child_offset)?;

                    let mut child_node = Node::try_from(child_page)?;

                    self.update_item(&data, &selection, &mut child_node, node_offset)?;
                }

                Ok(())
            }
            NodeType::Leaf(ref mut rows) => {
                for row in rows {
                    if !row.match_condition(&selection)? {
                        continue;
                    }

                    for item in data {
                        if let Some(v) = row.0.get_mut(item.1) {
                            *v = item.0.to_owned();
                        }
                    }
                }

                self.pager
                    .write_page_at_offset(Page::try_from(&*node)?, &node_offset)?;

                Ok(())
            }
            NodeType::Unexpected => return Err(Error::UnexpectedWithReason("Unknown node type.")),
        }
    }

    pub fn create_table(&mut self, schema: Schema) -> Result<(), Error> {
        //let root_page = self.pager.get_page(&Offset(0))?;
        let root_offset = Offset(256);

        let schema_node = Node::new(NodeType::Schema(schema), true, None);

        self.pager
            .write_page_at_offset(Page::try_from(&schema_node)?, &Offset(0))?;

        let node = Node::new(NodeType::Leaf(vec![]), true, None);

        self.pager.set_cursor(root_offset.0);

        let root = self.pager.write_page(Page::try_from(&node)?)?;

        self.wal.set_root(&root)
    }

    pub fn insert(&mut self, row: Record) -> Result<(), Error> {
        let root_offset = self.wal.get_root()?;

        let root_page = self.pager.get_page(&root_offset)?;

        let new_root_offset: Offset;
        let mut new_root: Node;
        let mut root = Node::try_from(root_page)?;

        if self.is_node_full(&root)? {
            new_root = Node::new(NodeType::Internal(vec![], vec![]), true, None);
            new_root_offset = self.pager.write_page(Page::try_from(&new_root)?)?;

            root.parent_offset = Some(new_root_offset.clone());
            root.is_root = false;

            let (median, sibling) = root.split(self.b)?;

            let old_root_offset = self.pager.write_page(Page::try_from(&root)?)?;
            let sibling_offset = self.pager.write_page(Page::try_from(&sibling)?)?;

            new_root.node_type =
                NodeType::Internal(vec![old_root_offset, sibling_offset], vec![median]);

            self.pager
                .write_page_at_offset(Page::try_from(&new_root)?, &new_root_offset)?;
        } else {
            new_root = root.clone();
            new_root_offset = self.pager.write_page(Page::try_from(&new_root)?)?;
        }

        self.insert_non_full(&mut new_root, new_root_offset.clone(), row)?;

        self.wal.set_root(&new_root_offset)
    }

    fn insert_non_full(
        &mut self,
        node: &mut Node,
        node_offset: Offset,
        row: Record,
    ) -> Result<(), Error> {
        match &mut node.node_type {
            NodeType::Schema(_) => {
                return Err(Error::UnexpectedWithReason(
                    "Should not insert on schema node",
                ))
            }
            NodeType::Internal(ref mut children, ref mut keys) => {
                let key = row.0.get(0).expect("Failed to get key").clone();
                let idx = keys.binary_search(&key.clone()).unwrap_or_else(|x| x);

                let child_offset = children
                    .get(0)
                    .ok_or(Error::UnexpectedWithReason("Failed to get child offset"))?
                    .clone();
                let child_page = self.pager.get_page(&child_offset)?;

                let mut child = Node::try_from(child_page)?;

                let new_child_offset = self.pager.write_page(Page::try_from(&child)?)?;

                children[idx] = new_child_offset.to_owned();

                if self.is_node_full(&child)? {
                    let (median, mut sibling) = child.split(self.b)?;
                    self.pager
                        .write_page_at_offset(Page::try_from(&child)?, &new_child_offset)?;

                    let sibling_offset = self.pager.write_page(Page::try_from(&sibling)?)?;
                    children.insert(idx + 1, sibling_offset.clone());

                    keys.insert(idx, median.clone());

                    self.pager
                        .write_page_at_offset(Page::try_from(&*node)?, &node_offset)?;

                    if key <= median {
                        self.insert_non_full(&mut child, new_child_offset, row)
                    } else {
                        self.insert_non_full(&mut sibling, sibling_offset, row)
                    }
                } else {
                    self.pager
                        .write_page_at_offset(Page::try_from(&*node)?, &node_offset)?;
                    self.insert_non_full(&mut child, new_child_offset, row)
                }
            }
            NodeType::Leaf(ref mut rows) => {
                //let idx = rows.binary_search().unwrap_or_else(|x| x);

                rows.push(row);

                self.pager
                    .write_page_at_offset(Page::try_from(&*node)?, &node_offset)
            }
            NodeType::Unexpected => Err(Error::UnexpectedWithReason(
                "Failed to insert into unknown node.",
            )),
        }
    }

    /*pub fn search(&mut self, key: Value) -> Result<Record, Error> {
        let root_offset = self.wal.get_root()?;
        let root_page = self.pager.get_page(&root_offset)?;
        let root = Node::try_from(root_page)?;
        self.search_node(root, key)
    }

    fn search_node(&mut self, node: Node, search: Value) -> Result<Record, Error> {
        match node.node_type {
            NodeType::Schema(schmea) => {
                if let Some(child_offset) = schmea.get_child_offset() {
                    let page = self.pager.get_page(&child_offset)?;
                    let child_node = Node::try_from(page)?;

                    self.search_node(child_node, search)
                } else {
                    Err(Error::NotFound)
                }
            }
            NodeType::Internal(children, keys) => {
                let idx = keys.binary_search(&search).unwrap_or_else(|x| x);

                let child_offset = children.get(idx).ok_or(Error::Unexpected)?;
                let page = self.pager.get_page(child_offset)?;
                let child_node = Node::try_from(page)?;

                self.search_node(child_node, search)
            }
            NodeType::Leaf(rows) => {
                if let Ok(idx) = rows.binary_search_by_key(&search, |x| {
                    x.0.get(0).expect("Failed to get id").clone()
                }) {
                    return Ok(rows[idx].clone());
                }

                return Err(Error::NotFound);
            }
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }*/

    fn parse_conditions(
        &mut self,
        condition: &Vec<Condition>,
    ) -> Result<Vec<ConditionValue>, Error> {
        let schema = self.get_table()?;

        let mut result = vec![];
        let mut invert = false;
        for x in condition {
            match x {
                Condition::E(column, value) => {
                    let (col_value, idx) = schema.parse_value_by_col(&column, &value)?;

                    result.push(ConditionValue::Value {
                        invert: invert,
                        idx: idx,
                        opt: Operation::Equal,
                        values: [col_value, Value::Null],
                    });
                    invert = false;
                }
                Condition::GT(column, value) => {
                    let (col_value, idx) = schema.parse_value_by_col(&column, &value)?;

                    result.push(ConditionValue::Value {
                        invert: invert,
                        idx: idx,
                        opt: Operation::GT,
                        values: [col_value, Value::Null],
                    });
                    invert = false;
                }
                Condition::LT(column, value) => {
                    let (col_value, idx) = schema.parse_value_by_col(&column, &value)?;

                    result.push(ConditionValue::Value {
                        invert: invert,
                        idx: idx,
                        opt: Operation::LT,
                        values: [col_value, Value::Null],
                    });
                    invert = false;
                }
                Condition::GTE(column, value) => {
                    let (col_value, idx) = schema.parse_value_by_col(&column, &value)?;

                    result.push(ConditionValue::Value {
                        invert: invert,
                        idx: idx,
                        opt: Operation::GTE,
                        values: [col_value, Value::Null],
                    });
                    invert = false;
                }
                Condition::LTE(column, value) => {
                    let (col_value, idx) = schema.parse_value_by_col(&column, &value)?;

                    result.push(ConditionValue::Value {
                        invert: invert,
                        idx: idx,
                        opt: Operation::LTE,
                        values: [col_value, Value::Null],
                    });
                    invert = false;
                }
                Condition::NE(column, value) => {
                    let (col_value, idx) = schema.parse_value_by_col(&column, &value)?;

                    result.push(ConditionValue::Value {
                        invert: !invert,
                        idx: idx,
                        opt: Operation::Equal,
                        values: [col_value, Value::Null],
                    });
                    invert = false;
                }
                Condition::NOT => {
                    invert = true;
                }
                Condition::BETWEEN(column, range_start, range_end) => {
                    let (start_col, idx) = schema.parse_value_by_col(&column, &range_start)?;
                    let (end_col, _) = schema.parse_value_by_col(&column, &range_end)?;
                    result.push(ConditionValue::Value {
                        invert: invert,
                        idx: idx,
                        opt: Operation::BETWEEN,
                        values: [start_col, end_col],
                    });
                    invert = false;
                }
                Condition::LIKE(column, value) => {
                    let (col_value, idx) = schema.parse_value_by_col(&column, &value)?;

                    result.push(ConditionValue::Value {
                        invert: invert,
                        idx: idx,
                        opt: Operation::LIKE,
                        values: [col_value, Value::Null],
                    });
                    invert = false;
                }
                Condition::AND => result.push(ConditionValue::AND),
                Condition::OR => result.push(ConditionValue::OR),
            }
        }

        Ok(result)
    }

    pub fn delete(&mut self, condition: Option<&Vec<Condition>>) -> Result<(), Error> {
        let values = if let Some(cond) = condition {
            Some(self.parse_conditions(cond)?)
        } else {
            None
        };

        let root_offset = self.wal.get_root()?;
        let root_page = self.pager.get_page(&root_offset)?;

        let mut new_root = Node::try_from(root_page)?;
        let new_root_page = Page::try_from(&new_root)?;
        let new_root_offset = self.pager.write_page(new_root_page)?;

        self.delete_key_from_subtree(&values, &mut new_root, &new_root_offset)?;
        self.wal.set_root(&new_root_offset)
    }

    fn delete_key_from_subtree(
        &mut self,
        selection: &Option<Vec<ConditionValue>>,
        node: &mut Node,
        node_offset: &Offset,
    ) -> Result<(), Error> {
        match &mut node.node_type {
            NodeType::Schema(_) => {
                return Err(Error::UnexpectedWithReason("Cant not delete schema node"))
            }
            NodeType::Internal(children, _keys) => {
                //let node_idx = keys.binary_search(&key).unwrap_or_else(|x| x);

                for child_offset in children {
                    let child_page = self.pager.get_page(&child_offset)?;

                    let mut child_node = Node::try_from(child_page)?;

                    self.delete_key_from_subtree(&selection, &mut child_node, &child_offset)?;

                    //let child_page = self.pager.get_page(child_offset)?;
                    //let mut child_node = Node::try_from(child_page)?;
                    // child_node.parent_offset = Some(node_offset.to_owned());

                    //let new_child_page = Page::try_from(&child_node)?;
                    //et new_child_offset = self.pager.write_page(new_child_page)?;
                }

                //children[node_idx] = new_child_offset.to_owned();

                //self.pager
                // .write_page_at_offset(Page::try_from(&*node)?, node_offset)?;

                return Ok(());
            }
            NodeType::Leaf(ref mut rows) => {
                rows.retain(|x| !x.match_condition(selection).unwrap_or(false));

                /*let idx = rows
                    .binary_search_by_key(&key, |x| x.0.get(0).expect("").clone())
                    .map_err(|_| Error::NotFound)?;

                rows.remove(idx);*/

                self.pager
                    .write_page_at_offset(Page::try_from(&*node)?, node_offset)?;

                //self.borrow_if_needed(node.to_owned(), &key)?;
            }
            NodeType::Unexpected => return Err(Error::Unexpected),
        }

        Ok(())
    }

    /*fn borrow_if_needed(&mut self, node: Node, key: &Value) -> Result<(), Error> {
            if self.is_node_underflow(&node)? {
                let parent_offset = node.parent_offset.clone().ok_or(Error::Unexpected)?;
                let parent_page = self.pager.get_page(&parent_offset)?;
                let mut parent_node = Node::try_from(parent_page)?;

                match parent_node.node_type {
                    NodeType::Schema(_) => {
                        return Err(Error::UnexpectedWithReason("Cant not borrow schema node"))
                    }
                    NodeType::Internal(ref mut children, ref mut keys) => {
                        let idx = keys.binary_search(key).unwrap_or_else(|x| x);

                        let sibling_idx = match idx > 0 {
                            true => idx + 1,
                            false => idx - 1,
                        };

                        let sibling_offset = children.get(sibling_idx).ok_or(Error::Unexpected)?;
                        let sibling_page = self.pager.get_page(sibling_offset)?;
                        let sibling = Node::try_from(sibling_page)?;
                        let mereged_node = self.merge(node, sibling)?;
                        let merged_node_offset =
                            self.pager.write_page(Page::try_from(&mereged_node)?)?;
                        let merged_node_idx = std::cmp::min(idx, sibling_idx);

                        children.remove(merged_node_idx);

                        children.remove(merged_node_idx);

                        if parent_node.is_root && children.is_empty() {
                            self.wal.set_root(&merged_node_offset)?;
                            return Ok(());
                        }

                        keys.remove(idx);

                        children.insert(merged_node_idx, merged_node_offset);

                        self.pager
                            .write_page_at_offset(Page::try_from(&parent_node)?, &parent_offset)?;

                        return self.borrow_if_needed(parent_node, key);
                    }
                    _ => return Err(Error::Unexpected),
                }
            }

            Ok(())
        }

        fn merge(&self, first: Node, second: Node) -> Result<Node, Error> {
            match first.node_type {
                NodeType::Schema(_) => Err(Error::UnexpectedWithReason("Can not merge schema node")),
                NodeType::Internal(first_offset, first_keys) => {
                    if let NodeType::Internal(second_offsets, second_keys) = second.node_type {
                        let merged_keys: Vec<Value> = first_keys
                            .into_iter()
                            .chain(second_keys.into_iter())
                            .collect();

                        let merged_offsets: Vec<Offset> = first_offset
                            .into_iter()
                            .chain(second_offsets.into_iter())
                            .collect();

                        let node_type = NodeType::Internal(merged_offsets, merged_keys);
                        return Ok(Node::new(node_type, first.is_root, first.parent_offset));
                    }
                    Err(Error::Unexpected)
                }
                NodeType::Leaf(first_row) => {
                    if let NodeType::Leaf(second_row) = second.node_type {
                        let merged_row: Vec<Record> = first_row
                            .into_iter()
                            .chain(second_row.into_iter())
                            .collect();
                        let node_type = NodeType::Leaf(merged_row);
                        return Ok(Node::new(node_type, first.is_root, first.parent_offset));
                    }
                    Err(Error::Unexpected)
                }
                NodeType::Unexpected => Err(Error::Unexpected),
            }
        }
    */
    /*fn print_sub_tree(&mut self, prefix: String, offset: Offset) -> Result<(), Error> {
        println!("{} Node at offset: {}", prefix, offset.0);

        let curr_prefix = format!("{} |->", prefix);
        let page = self.pager.get_page(&offset)?;
        let node = Node::try_from(page)?;

        match node.node_type {
            NodeType::Schema(_) => Err(Error::UnexpectedWithReason(
                "Should not have started with schema node.",
            )),
            NodeType::Internal(children, keys) => {
                println!("{} Keys {:?}", curr_prefix, keys);
                println!("{} Children: {:?}", curr_prefix, children);

                let child_prefix = format!("{}   |  ", prefix);
                for child_offset in children {
                    self.print_sub_tree(child_prefix.clone(), child_offset)?;
                }
                Ok(())
            }
            NodeType::Leaf(rows) => {
                println!("{} Rows {:?}", curr_prefix, rows);
                Ok(())
            }
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }

    pub fn print(&mut self) -> Result<(), Error> {
        println!();

        let schema = self.get_table()?;

        println!("=== TABLE {} ===", schema.name);

        for (idx, col) in schema.columns.iter().enumerate() {
            println!(
                "{}{}{}: {}",
                if schema.primary_key == idx {
                    "Primary Key: "
                } else {
                    ""
                },
                col.0,
                if col.2 == true { "?" } else { "" },
                Value::print_type(col.1)
            );
        }
        println!();

        let root_offset = self.wal.get_root()?;
        self.print_sub_tree("".to_string(), root_offset)
    }*/
}

impl BTreeBuilder {
    pub fn new() -> Self {
        Self {
            path: PathBuf::new(),
            b: 0,
            offset: 0,
        }
    }
    pub fn path(mut self, path: PathBuf) -> Self {
        self.path = path;
        self
    }
    pub fn b_parameter(mut self, b: usize) -> Self {
        self.b = b;
        self
    }

    pub fn cursor_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    pub fn build(&self) -> Result<BTree, Error> {
        if self.path.to_string_lossy() == "" {
            return Err(Error::UnexpectedWithReason("File path is empty"));
        }

        if self.b == 0 {
            return Err(Error::UnexpectedWithReason(
                "b paramter can not be less then 0.",
            ));
        }

        let mut pager = Pager::new(self.path.clone())?;

        if self.offset != 0 {
            pager.set_cursor(256)
        }

        //let mut pager = ?;

        let parent_directory = self
            .path
            .parent()
            .ok_or_else(|| Error::UnexpectedWithReason("Failed to get parent of given path."))?;

        Ok(BTree {
            pager,
            b: self.b,
            wal: Wal::new(parent_directory.to_path_buf())?,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::engine::structure::Value;

    fn get_db() -> BTree {
        let tree = match BTreeBuilder::new()
            .b_parameter(10)
            .path(PathBuf::from("./db/test.bin"))
            .cursor_offset(256)
            .build()
        {
            Ok(value) => value,
            Err(e) => panic!("{}", e),
        };

        tree
    }

    #[test]
    fn test_create_table() {
        let mut db = get_db();

        let schema = Schema::new("Users".into(), 0, vec![], None);

        if let Err(err) = db.create_table(schema) {
            panic!("{}", err);
        }
    }

    #[test]
    fn test_insert() {
        let mut tree = get_db();

        if let Err(e) = tree.insert(Record(vec![
            Value::U64(1),
            Value::String("Hello".into()),
            Value::Null,
        ])) {
            panic!("{}", e);
        }
    }
}
