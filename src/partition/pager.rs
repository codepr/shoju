use std::collections::{HashMap, HashSet};
use std::fs::File;

const PAGESIZE: usize = 4096;
const BUFSIZE: usize = 8192;
const MIN_PAGES: usize = 4;
const PAGES_PER_SHARD: usize = 32;
const MAX_SHARDS: usize = 128;

struct Pager {
    file: File,
    page_size: usize,
    page_max_size: usize,
    size: usize,
    shards: Vec<Shard>,
}

#[derive(Copy)]
struct Page {
    num: usize,
    prev: Option<Box<Self>>,
    next: Option<Box<Self>>,
    data: Vec<u8>,
}

struct Shard {
    pages: HashMap<usize, Page>,
    dirty: HashSet<usize>,
    head: Option<Box<Page>>,
    tail: Option<Box<Page>>,
}

impl Shard {
    pub fn push(&mut self, page: Option<Box<Page>>) {
        self.head.unwrap().prev = page;
        page.unwrap().next = self.head.unwrap().next;
        page.unwrap().prev = self.head;
    }
}
