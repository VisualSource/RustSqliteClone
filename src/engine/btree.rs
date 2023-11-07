use super::{
    error::Error,
    node::Node,
    node_type::{self, NodeType},
    page::Page,
    pager::Pager,
    structure::{Offset, Record, Value},
    wal::Wal,
};
use std::{path::Path, vec};

pub struct BTree {
    pager: Pager,
    b: usize,
    wal: Wal,
}

pub struct BTreeBuilder {
    path: &'static Path,
    b: usize,
}

impl BTree {
    fn is_node_full(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            NodeType::Schema(_, keys) => Ok(keys.len() == (2 * self.b - 1)),
            NodeType::Leaf(rows) => Ok(rows.len() == (2 * self.b - 1)),
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }

    fn is_node_underflow(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            NodeType::Schema(_, keys) => Ok(keys.len() < self.b - 1 && !node.is_root),
            NodeType::Leaf(rows) => Ok(rows.len() < self.b - 1 && !node.is_root),
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }

    pub fn insert(&mut self, row: Record) -> Result<(), Error> {
        let root_offset = self.wal.get_root()?;
        let root_page = self.pager.get_page(&root_offset)?;

        let new_root_offset: Offset;
        let mut new_root: Node;
        let mut root = Node::try_from(root_page)?;

        if self.is_node_full(&root)? {
            new_root = Node::new(NodeType::Schema(vec![], vec![]), true, None);
            new_root_offset = self.pager.write_page(Page::try_from(&new_root)?)?;

            root.parent_offset = Some(new_root_offset.clone());
            root.is_root = false;

            let (median, sibling) = root.split(self.b)?;

            let old_root_offset = self.pager.write_page(Page::try_from(&root)?)?;
            let sibling_offset = self.pager.write_page(Page::try_from(&sibling)?)?;

            new_root.node_type =
                NodeType::Schema(vec![old_root_offset, sibling_offset], vec![median]);

            self.pager
                .write_page_at_offset(Page::try_from(&new_root)?, &new_root_offset)?;
        } else {
            new_root = root.clone();
            new_root_offset = self.pager.write_page(Page::try_from(&new_root)?)?;
        }

        self.insert_non_full(&mut new_root, new_root_offset.clone(), row)?;

        self.wal.set_root(new_root_offset)
    }

    fn insert_non_full(
        &mut self,
        node: &mut Node,
        node_offset: Offset,
        row: Record,
    ) -> Result<(), Error> {
        match &mut node.node_type {
            NodeType::Schema(ref mut children, ref mut keys) => {
                let key = row.0.get(0).expect("Failed to get key").clone();
                let idx = keys.binary_search(&key.clone()).unwrap_or_else(|x| x);

                let child_offset = children.get(0).ok_or(Error::Unexpected)?.clone();
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
                let idx = rows.binary_search(&row).unwrap_or_else(|x| x);

                rows.insert(idx, row);

                self.pager
                    .write_page_at_offset(Page::try_from(&*node)?, &node_offset)
            }
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }

    pub fn search(&mut self, key: Value) -> Result<Record, Error> {
        let root_offset = self.wal.get_root()?;
        let root_page = self.pager.get_page(&root_offset)?;
        let root = Node::try_from(root_page)?;
        self.search_node(root, key)
    }

    fn search_node(&mut self, node: Node, search: Value) -> Result<Record, Error> {
        match node.node_type {
            NodeType::Schema(children, keys) => {
                let idx = keys.binary_search(&search).unwrap_or_else(|x| x);

                let child_offset = children.get(0).ok_or(Error::Unexpected)?;
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
            NodeType::Unexpected => todo!(),
        }
    }

    pub fn delete(&mut self, key: Value) -> Result<(), Error> {
        let root_offset = self.wal.get_root()?;
        let root_page = self.pager.get_page(&root_offset)?;

        let mut new_root = Node::try_from(root_page)?;
        let new_root_page = Page::try_from(&new_root)?;
        let new_root_offset = self.pager.write_page(new_root_page)?;

        self.delete_key_from_subtree(key, &mut new_root, &new_root_offset)?;
        self.wal.set_root(new_root_offset)
    }

    fn delete_key_from_subtree(
        &mut self,
        key: Value,
        node: &mut Node,
        node_offset: &Offset,
    ) -> Result<(), Error> {
        match &mut node.node_type {
            NodeType::Schema(children, keys) => {
                let node_idx = keys.binary_search(&key).unwrap_or_else(|x| x);

                let child_offset = children.get(node_idx).ok_or(Error::Unexpected)?;

                let child_page = self.pager.get_page(child_offset)?;
                let mut child_node = Node::try_from(child_page)?;

                child_node.parent_offset = Some(node_offset.to_owned());

                let new_child_page = Page::try_from(&child_node)?;
                let new_child_offset = self.pager.write_page(new_child_page)?;

                children[node_idx] = new_child_offset.to_owned();

                self.pager
                    .write_page_at_offset(Page::try_from(&*node)?, node_offset)?;

                return self.delete_key_from_subtree(key, &mut child_node, &new_child_offset);
            }
            NodeType::Leaf(ref mut rows) => {
                let idx = rows
                    .binary_search_by_key(&key, |x| x.0.get(0).expect("").clone())
                    .map_err(|_| Error::NotFound)?;

                rows.remove(idx);

                self.pager
                    .write_page_at_offset(Page::try_from(&*node)?, node_offset)?;

                self.borrow_if_needed(node.to_owned(), &key)?;
            }
            NodeType::Unexpected => todo!(),
        }

        Ok(())
    }

    fn borrow_if_needed(&mut self, node: Node, key: &Value) -> Result<(), Error> {
        if self.is_node_underflow(&node)? {
            let parent_offset = node.parent_offset.clone().ok_or(Error::Unexpected)?;
            let parent_page = self.pager.get_page(&parent_offset)?;
            let mut parent_node = Node::try_from(parent_page)?;

            match parent_node.node_type {
                NodeType::Schema(ref mut children, ref mut keys) => {
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
                        self.wal.set_root(merged_node_offset)?;
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
            NodeType::Schema(first_offset, first_keys) => {
                if let NodeType::Schema(second_offsets, second_keys) = second.node_type {
                    let merged_keys: Vec<Value> = first_keys
                        .into_iter()
                        .chain(second_keys.into_iter())
                        .collect();

                    let merged_offsets: Vec<Offset> = first_offset
                        .into_iter()
                        .chain(second_offsets.into_iter())
                        .collect();

                    let node_type = NodeType::Schema(merged_offsets, merged_keys);
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
}

impl BTreeBuilder {
    pub fn new() -> Self {
        Self {
            path: Path::new(""),
            b: 0,
        }
    }
    pub fn path(mut self, path: &'static Path) -> Self {
        self.path = path;
        self
    }
    pub fn b_parameter(mut self, b: usize) -> Self {
        self.b = b;
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

        let mut pager = Pager::new(self.path)?;
        let root = Node::new(NodeType::Leaf(vec![]), true, None);
        let root_offset = pager.write_page(Page::try_from(&root)?)?;

        let parent_directory = self.path.parent().unwrap_or_else(|| Path::new("/tmp"));

        let mut wal = Wal::new(parent_directory.to_path_buf())?;
        wal.set_root(root_offset)?;

        Ok(BTree {
            pager,
            b: self.b,
            wal,
        })
    }
}

impl Default for BTreeBuilder {
    fn default() -> Self {
        BTreeBuilder::new()
            .b_parameter(200)
            .path(Path::new("/tmp/db"))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_insert_get() {
        let mut tree = match BTreeBuilder::new()
            .b_parameter(10)
            .path(Path::new("./db/test.bin"))
            .build()
        {
            Ok(value) => value,
            Err(e) => panic!("{}", e),
        };

        if let Err(e) = tree.insert(Record(vec![Value::U64(0), Value::String("Test".into())])) {
            panic!("{}", e);
        }

        match tree.search(Value::U64(0)) {
            Ok(v) => {
                println!("{:#?}", v);
            }
            Err(e) => panic!("{}", e),
        }
    }
    
    #[test]
    fn test_get() {
        let mut tree = match BTreeBuilder::new()
            .b_parameter(10)
            .path(Path::new("./db/test.bin"))
            .build()
        {
            Ok(value) => value,
            Err(e) => panic!("{}", e),
        };

        match tree.search(Value::U64(1)) {
            Ok(v) => println!("{:#?}", v),
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_insert() -> Result<(), Error> {
        let mut tree = match BTreeBuilder::new()
            .b_parameter(10)
            .path(Path::new("./db/test.bin"))
            .build()
        {
            Ok(v) => v,
            Err(e) => panic!("{}", e),
        };

        if let Err(e) = tree.insert(Record(vec![
            Value::U64(0),
            Value::String("Hello There".into()),
        ])) {
            panic!("{}", e);
        }

        if let Err(e) = tree.insert(Record(vec![Value::U64(1), Value::String("Yo".into())])) {
            panic!("{}", e);
        }

        //println!("{:#?}", tree.search(Value::U64(0)));

        Ok(())
    }
}
