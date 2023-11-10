use super::{error::Error, node_type::Offset, page_layout::PTR_SIZE};

use std::{
    convert::TryFrom,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn new(parent_directoy: PathBuf) -> Result<Self, Error> {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(parent_directoy.join("wal"))?;

        Ok(Self { file: fd })
    }

    pub fn get_root(&mut self) -> Result<Offset, Error> {
        let mut buff: [u8; PTR_SIZE] = [0x00; PTR_SIZE];
        let file_len = self.file.seek(SeekFrom::End(0))? as usize;

        println!("Walk file len: {file_len}");
        let root_offset: usize = if file_len > 0 {
            (file_len / PTR_SIZE - 1) * PTR_SIZE
        } else {
            0
        };

        println!("Walk file root offset: {root_offset}");
        self.file.seek(SeekFrom::Start(root_offset as u64))?;
        self.file.read_exact(&mut buff)?;
        Offset::try_from(buff)
    }

    pub fn set_root(&mut self, offset: Offset) -> Result<(), Error> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&offset.0.to_be_bytes())?;
        Ok(())
    }
}
