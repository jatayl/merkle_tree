use std::cell::RefCell;
use std::mem;
use std::rc::Rc;

use blake3::Hash;
use serde::{Deserialize, Serialize};

#[inline]
pub(crate) fn log2(n: usize) -> usize {
    const POINTER_WIDTH: usize = mem::size_of::<usize>() * 8;
    POINTER_WIDTH - n.leading_zeros() as usize - 1
}

#[inline]
pub(crate) fn partition(n: usize) -> usize {
    let leading_1_mask = 1 << log2(n);
    if leading_1_mask == n {
        leading_1_mask >> 1
    } else {
        leading_1_mask
    }
}

pub(crate) fn path_to_root(index: usize, tree_size: usize) -> (usize, usize) {
    let partition = partition(tree_size);
    if partition == 0 {
        // then there is only one node in this subtree
        return (0, 1);
    }
    if tree_size == (partition << 1) || index < partition {
        // its either a full tree
        // or in the left subtree
        // both are full and have path to root same as index
        return (index, log2(partition) + 1);
    }
    // otherwise, take a look on the right subtree (other side of partition)
    let (back, size) = path_to_root(index - partition, tree_size - partition);
    // put one in front because traverse from right
    ((1 << size) + back, size + 1)
}

// smuggle Rc's across threads
pub(crate) struct Ratn<T> {
    inner: Rc<RefCell<T>>,
}

impl<T> Ratn<T> {
    pub(crate) fn new(inner: Rc<RefCell<T>>) -> Self {
        Self { inner }
    }

    pub(crate) unsafe fn into_inner(self) -> Rc<RefCell<T>> {
        self.inner
    }
}

// Safety: For this problem, threads do not ever use the same Rc at the same time
// Thus, we are okay making them Send
unsafe impl<T> Send for Ratn<T> {}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Hash")]
pub(crate) struct HashDef {
    #[serde(getter = "Hash::as_bytes")]
    bytes: [u8; 32],
}

impl From<HashDef> for Hash {
    fn from(hash: HashDef) -> Hash {
        Hash::from_hex(hash.bytes).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_helper() {
        assert_eq!(partition(0b0101), 0b0100);
        assert_eq!(partition(0b0100), 0b0010);
        assert_eq!(partition(0b1111), 0b1000);
        assert_eq!(partition(0b1000), 0b0100);
    }

    #[test]
    fn path_to_root_helper() {
        assert_eq!(path_to_root(0b100, 0b110), (0b010, 2));
        assert_eq!(path_to_root(0b010, 0b110), (0b010, 3));
        assert_eq!(path_to_root(0b100, 0b111), (0b100, 3));
        assert_eq!(path_to_root(0b000, 0b001), (0b000, 1));
    }
}
