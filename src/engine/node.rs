use super::{
    error::Error,
    node_type::NodeType,
    page::Page,
    page_layout::{
        FromByte, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_NUM_CHILDREN_OFFSET, IS_ROOT_OFFSET,
        NODE_TYPE_OFFSET, PARENT_PONTER_OFFSET, PTR_SIZE, ROW_NUM_OFFSET,
    },
    structure::{Offset, Record, Value},
};

#[derive(Debug, Clone)]
pub struct Node {
    pub node_type: NodeType,
    pub is_root: bool,
    pub parent_offset: Option<Offset>,
}

impl Node {
    pub fn new(node_type: NodeType, is_root: bool, parent_offset: Option<Offset>) -> Self {
        Self {
            node_type,
            is_root,
            parent_offset,
        }
    }

    pub fn split(&mut self, b: usize) -> Result<(Value, Node), Error> {
        match self.node_type {
            NodeType::Schema(ref mut children, ref mut keys) => {
                let mut sibling_keys = keys.split_off(b - 1);
                let median_key = sibling_keys.remove(0);
                let sibling_children = children.split_off(b);

                Ok((
                    median_key,
                    Node::new(
                        NodeType::Schema(sibling_children, sibling_keys),
                        false,
                        self.parent_offset.clone(),
                    ),
                ))
            }
            NodeType::Leaf(ref mut rows) => {
                let sibling_rows = rows.split_off(b);

                let median_pair = rows.get(b - 1).ok_or(Error::Unexpected)?.clone();

                Ok((
                    median_pair.0.get(0).expect("Failed to get key").clone(),
                    Node::new(
                        NodeType::Leaf(sibling_rows),
                        false,
                        self.parent_offset.clone(),
                    ),
                ))
            }
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }
}

impl TryFrom<Page> for Node {
    type Error = Error;
    fn try_from(page: Page) -> Result<Self, Self::Error> {
        let raw = page.get_data();

        let is_root = raw[IS_ROOT_OFFSET].from_byte();
        let node_type = NodeType::from(raw[NODE_TYPE_OFFSET]);

        let parent_offset = if is_root {
            None
        } else {
            Some(Offset(page.get_value_from_offset(PARENT_PONTER_OFFSET)?))
        };
        let config = bincode::config::standard();
        match node_type {
            NodeType::Schema(mut children, mut keys) => {
                let num_children = page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;

                let mut offset = INTERNAL_NODE_HEADER_SIZE;

                for _ in 1..=num_children {
                    let child_offset = page.get_value_from_offset(offset)?;
                    children.push(Offset(child_offset));
                    offset += PTR_SIZE;
                }

                let data_len = page.get_value_from_offset(offset)?;

                offset += PTR_SIZE;

                let buffer = page.get_ptr_from_offset(offset, data_len);

                let (data, content_len): (Vec<Value>, usize) =
                    bincode::serde::decode_from_slice(&buffer, config)?;

                if data_len != content_len {
                    return Err(Error::UnexpectedWithReason(
                        "Data len does not match decoded data len.",
                    ));
                }

                offset += content_len;

                Ok(Node::new(
                    NodeType::Schema(children, keys),
                    is_root,
                    parent_offset,
                ))
            }
            NodeType::Leaf(mut rows) => {
                let mut offset = ROW_NUM_OFFSET;
                let num_of_rows = page.get_value_from_offset(offset)?;

                offset += PTR_SIZE;

                for _ in 0..num_of_rows {
                    let data_len = page.get_value_from_offset(offset)?;

                    offset += PTR_SIZE;

                    let buffer = page.get_ptr_from_offset(offset, data_len);

                    let (data, data_length): (Record, usize) =
                        bincode::serde::decode_from_slice(&buffer, config)?;

                    if data_len != data_length {
                        return Err(Error::Unexpected);
                    }

                    offset += data_length;

                    rows.push(data);
                }

                Ok(Node::new(NodeType::Leaf(rows), is_root, parent_offset))
            }
            NodeType::Unexpected => Err(Error::Unexpected),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        error::Error,
        node_type::NodeType,
        page::Page,
        page_layout::PAGE_SIZE,
        structure::{Record, Value},
    };

    use super::Node;

    #[test]
    fn page_to_node_leaf() -> Result<(), Error> {
        let config = bincode::config::standard();

        let page_data: [u8; 18] = [
            0x01, // is root
            0x02, // node type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // num of cols
        ];

        let item = Record(vec![
            Value::String("Hello".into()),
            Value::String(",World".into()),
        ]);

        let data = bincode::serde::encode_to_vec(&item, config)?;

        let junk = vec![0x00; PAGE_SIZE - 18 - data.len() - 8];

        let mut page = [0x00; PAGE_SIZE];

        for (to, from) in page.iter_mut().zip(
            page_data
                .iter()
                .chain(data.len().to_be_bytes().iter())
                .chain(data.iter())
                .chain(junk.iter()),
        ) {
            *to = *from
        }

        let node = match Node::try_from(Page::new(page)) {
            Ok(value) => value,
            Err(err) => panic!("{}", err.to_string()),
        };

        println!("{:#?}", node);

        assert_eq!(node.is_root, true);
        assert_eq!(node.node_type, NodeType::Leaf(vec![item]));
        assert_eq!(node.parent_offset, None);

        Ok(())
    }
}
