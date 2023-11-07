mod btree;
mod error;
mod node;
mod node_type;
mod page;
mod page_layout;
mod pager;
mod structure;
mod wal;

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use std::os::windows::prelude::FileExt;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    enum Test {
        String(String),
        Null,
    }
    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Record(Vec<Test>);

    #[test]
    fn test_encode_enum() {
        let config = bincode::config::standard();
        let item = Test::Null;

        let encoded: Vec<u8> = bincode::serde::encode_to_vec(&item, config).unwrap();

        println!("Enum Len {}", encoded.len());
        let mut fd = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open("./enum_bin.bin")
            .unwrap();

        fd.seek_write(&encoded, 0).unwrap();

        //let mut buf = [0; 4];
        // fd.seek(SeekFrom::Start(0)).unwrap();
        //fd.read_exact(&mut buf).unwrap();
        //let decoded: (Test, usize) = bincode::serde::decode_from_slice(&encoded, config).unwrap();
        //println!("{:#?}", decoded);
    }

    #[test]
    fn test_decode() {
        let config = bincode::config::standard();
        let fd = std::fs::OpenOptions::new()
            .read(true)
            .open("./db.bin")
            .unwrap();

        let mut is_root = [0; 1];
        fd.seek_read(&mut is_root[..], 0).unwrap();

        println!("IS ROOT: {}", is_root[0] == 0x01);

        let mut node_type = [0; 1];
        fd.seek_read(&mut node_type, 1).unwrap();

        println!("NODE TYPE: {}", u8::from(node_type[0]));

        let mut offset = [0; 8];
        fd.seek_read(&mut offset, 2).unwrap();

        println!("PARENT OFFSET: {}", usize::from_be_bytes(offset));

        let mut row_num = [0; 8];
        fd.seek_read(&mut row_num, 2 + 8).unwrap();

        println!("NUM OF ROWS: {}", usize::from_be_bytes(row_num));

        let mut col_size = [0; 8];
        fd.seek_read(&mut col_size, 2 + 8 + 8).unwrap();
        let col_size_num: usize = usize::from_be_bytes(col_size);
        println!("DATA SIZE: {}", col_size_num);

        let mut data = vec![0; col_size_num];
        fd.seek_read(&mut data, 2 + 8 + 8 + 8).unwrap();

        let decoded: (Record, usize) = bincode::serde::decode_from_slice(&data, config).unwrap();

        println!("{:#?}", decoded);
    }

    #[test]
    fn test_encode() {
        // | IS-ROOT 1 byte | TYPE 1 byte | OFFSET - 8 bytes | rows - 8 byte
        // | LEN - 8 bytes | ROW #N - N bytes |

        let config = bincode::config::standard();
        let data = Record(vec![
            Test::String("hello".to_string()),
            Test::String("a".to_string()),
        ]);

        let encoded: Vec<u8> = bincode::serde::encode_to_vec(&data, config).unwrap();
        let data_len = encoded.len();

        let fd = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open("./db.bin")
            .unwrap();

        let num_rows: usize = 1;
        let offset: usize = 0;

        // is root;
        fd.seek_write(&[0x00; 1], 0).unwrap();
        // write type
        fd.seek_write(&[0x03; 1], 1).unwrap();
        // write offset
        fd.seek_write(&offset.to_be_bytes(), 2).unwrap();
        // write rows num
        fd.seek_write(&num_rows.to_be_bytes(), 2 + 8).unwrap();

        // data len
        fd.seek_write(&data_len.to_be_bytes(), 2 + 8 + 8).unwrap();
        // col data
        fd.seek_write(&encoded, 2 + 8 + 8 + 8).unwrap();
    }
}
