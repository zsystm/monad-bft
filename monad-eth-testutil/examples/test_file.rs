use std::{
    fs::{self, File},
    io::{LineWriter, Write},
};

fn main() {
    let mut f = File::create("new_file.txt").expect("no error");
    let mut file = LineWriter::new(f);
    for i in 0..100 {
        file.write("new line".as_bytes()).unwrap();
        file.write(b"\n").unwrap();
    }

    file.flush().unwrap();
}
