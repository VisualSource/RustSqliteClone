use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use super::{error::Error, page::Page, page_layout::PAGE_SIZE, structure::Offset};

pub struct Pager {
    file: File,
    curser: usize,
}

impl Pager {
    pub fn new(path: PathBuf) -> Result<Self, Error> {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            //.truncate(true)
            .open(path)?;

        Ok(Self {
            file: fd,
            curser: 0,
        })
    }

    pub fn set_cursor(&mut self, curser: usize) {
        self.curser = curser;
    }

    pub fn get_schema(&mut self) -> Result<Page, Error> {
        let mut page: [u8; 512] = [0x00; 512];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut page)?;

        let mut temp: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];

        for x in 0..512 {
            temp[x] = page[x];
        }

        Ok(Page::new(temp))
    }

    pub fn get_page(&mut self, offset: &Offset) -> Result<Page, Error> {
        let mut page: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(offset.0 as u64))?;
        self.file.read_exact(&mut page)?;
        Ok(Page::new(page))
    }

    pub fn write_page(&mut self, page: Page) -> Result<Offset, Error> {
        self.file.seek(SeekFrom::Start(self.curser as u64))?;
        self.file.write_all(&page.get_data())?;
        let res = Offset(self.curser);
        self.curser += PAGE_SIZE;
        Ok(res)
    }

    pub fn write_page_at_offset(&mut self, page: Page, offset: &Offset) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(offset.0 as u64))?;
        self.file.write_all(&page.get_data())?;
        Ok(())
    }
}
