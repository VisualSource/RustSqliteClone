use super::{
    error::Error,
    node::Node,
    page_layout::{
        ToByte, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_NUM_CHILDREN_OFFSET,
        INTERNAL_NODE_NUM_CHILDREN_SIZE, IS_ROOT_OFFSET, NODE_TYPE_OFFSET, PAGE_SIZE,
        PARENT_POINTER_SIZE, PARENT_PONTER_OFFSET, PTR_SIZE, ROW_NUM_OFFSET, ROW_NUM_SIZE,
    },
    structure::{Offset, Usize},
};

pub struct Page {
    data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    pub fn new(data: [u8; PAGE_SIZE]) -> Self {
        Self {
            data: Box::new(data),
        }
    }

    pub fn write_value_at_offset(&mut self, offset: usize, value: usize) -> Result<(), Error> {
        if offset > PAGE_SIZE - PTR_SIZE {
            return Err(Error::OffsetOverflow);
        }

        let bytes = value.to_be_bytes();
        self.data[offset..offset + PTR_SIZE].clone_from_slice(&bytes);
        Ok(())
    }

    pub fn get_value_from_offset(&self, offset: usize) -> Result<usize, Error> {
        let bytes = &self.data[offset..offset + PTR_SIZE];

        let Usize(res) = Usize::try_from(bytes)?;
        Ok(res)
    }

    pub fn insert_bytes_at_offset(
        &mut self,
        bytes: &[u8],
        offset: usize,
        end_offset: usize,
        size: usize,
    ) -> Result<(), Error> {
        if end_offset + size > self.data.len() {
            return Err(Error::Unexpected);
        }

        for idx in (offset..=end_offset).rev() {
            self.data[idx + size] = self.data[idx];
        }

        self.data[offset..offset + size].clone_from_slice(bytes);
        Ok(())
    }

    pub fn write_bytes_at_offset(
        &mut self,
        bytes: &[u8],
        offset: usize,
        size: usize,
    ) -> Result<(), Error> {
        self.data[offset..offset + size].clone_from_slice(bytes);
        Ok(())
    }

    pub fn get_ptr_from_offset(&self, offset: usize, size: usize) -> &[u8] {
        &self.data[offset..offset + size]
    }

    pub fn get_data(&self) -> [u8; PAGE_SIZE] {
        *self.data
    }
}

impl TryFrom<&Node> for Page {
    type Error = Error;

    fn try_from(node: &Node) -> Result<Self, Self::Error> {
        let config = bincode::config::standard();
        let mut data: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];

        data[IS_ROOT_OFFSET] = node.is_root.to_byte();
        data[NODE_TYPE_OFFSET] = u8::from(&node.node_type);

        // | IS-ROOT 1 byte | TYPE 1 byte | OFFSET - 8 bytes | rows - 8 byte

        if !node.is_root {
            match node.parent_offset {
                Some(Offset(parent_offset)) => data
                    [PARENT_PONTER_OFFSET..PARENT_PONTER_OFFSET + PARENT_POINTER_SIZE]
                    .clone_from_slice(&parent_offset.to_be_bytes()),
                None => return Err(Error::Unexpected),
            }
        }

        match &node.node_type {
            super::node_type::NodeType::Schema(offsets, keys) => {
                data[INTERNAL_NODE_NUM_CHILDREN_OFFSET
                    ..INTERNAL_NODE_NUM_CHILDREN_OFFSET + INTERNAL_NODE_NUM_CHILDREN_SIZE]
                    .clone_from_slice(&offsets.len().to_be_bytes());

                let mut page_offset = INTERNAL_NODE_HEADER_SIZE;

                for Offset(child_offset) in offsets {
                    data[page_offset..page_offset + PTR_SIZE]
                        .clone_from_slice(&child_offset.to_be_bytes());
                    page_offset += PTR_SIZE;
                }

                let encoded_keys = bincode::serde::encode_to_vec(keys, config)?;
                let len = encoded_keys.len();

                data[page_offset..page_offset + PTR_SIZE].clone_from_slice(&len.to_be_bytes());

                page_offset += PTR_SIZE;

                data[page_offset..page_offset + len].clone_from_slice(&encoded_keys);

                page_offset += len
            }
            super::node_type::NodeType::Leaf(rows) => {
                data[ROW_NUM_OFFSET..ROW_NUM_OFFSET + ROW_NUM_SIZE]
                    .clone_from_slice(&rows.len().to_be_bytes());

                let mut page_offset = ROW_NUM_SIZE + ROW_NUM_OFFSET;
                for row in rows {
                    let enconded_data = bincode::serde::encode_to_vec(row, config)?;
                    let data_len = enconded_data.len();

                    // data size
                    data[page_offset..page_offset + PTR_SIZE]
                        .clone_from_slice(&data_len.to_be_bytes());

                    page_offset += PTR_SIZE;

                    data[page_offset..page_offset + data_len].clone_from_slice(&enconded_data);

                    page_offset += data_len
                }
            }
            super::node_type::NodeType::Unexpected => return Err(Error::Unexpected),
        }

        Ok(Page::new(data))
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        error::Error,
        node::Node,
        node_type::NodeType,
        structure::{Record, Value},
    };

    use super::Page;

    #[test]
    fn node_to_page_leaf() -> Result<(), Error> {
        let leaf = Node::new(
            NodeType::Leaf(vec![Record(vec![Value::String("hello".into())])]),
            true,
            None,
        );

        let page = Page::try_from(&leaf)?;

        let res = Node::try_from(page)?;

        assert_eq!(res.is_root, leaf.is_root);
        assert_eq!(res.node_type, leaf.node_type);
        assert_eq!(res.parent_offset, leaf.parent_offset);

        Ok(())
    }
}
