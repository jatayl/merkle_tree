mod join;

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash;
use std::mem::{self, MaybeUninit};
use std::rc::Rc;

use blake3::{Hash, Hasher};
use crossbeam::channel::{self, Sender};
use serde::Serialize;

// maybe use slab instead. that would be cool

pub trait Branchable: Clone + Copy + Serialize {
    fn fold(l: &Self, r: &Self) -> Self;
}

pub trait Merklable: Send + Sync + Clone + Copy + Serialize {
    type Branch: Branchable;
    type ID: Eq + hash::Hash;
    fn to_branch(&self) -> Self::Branch;
    fn id(&self) -> Self::ID;
}

struct Branch<M: Merklable> {
    inner: M::Branch,
    hash: Hash,
    parent: Option<Rc<RefCell<Branch<M>>>>,
    left_child: Child<M>,
    right_child: Option<Child<M>>,
}

impl<M: Merklable> Branch<M> {
    fn new(
        inner: M::Branch,
        hash: Hash,
        parent: Option<Rc<RefCell<Branch<M>>>>,
        left_child: Child<M>,
        right_child: Option<Child<M>>,
    ) -> Self {
        Branch {
            inner,
            hash,
            parent,
            left_child,
            right_child,
        }
    }

    fn set_parent(&mut self, b: Rc<RefCell<Branch<M>>>) {
        self.parent = Some(b);
    }
}

#[derive(Clone)]
pub struct Leaf<M: Merklable> {
    inner: M,
    hash: Hash,
    parent: Rc<RefCell<Branch<M>>>,
}

impl<M: Merklable> Leaf<M> {
    fn new(inner: M, hash: Hash, parent: Rc<RefCell<Branch<M>>>) -> Self {
        Leaf {
            inner,
            hash,
            parent,
        }
    }

    fn set_parent(&mut self, parent: Rc<RefCell<Branch<M>>>) {
        self.parent = parent;
    }
}

enum Child<M: Merklable> {
    Branch(Rc<RefCell<Branch<M>>>),
    Leaf(Rc<RefCell<Leaf<M>>>),
}

pub struct Proof<M: Merklable> {
    data: M,
    blocks: Vec<(M::Branch, Hash)>,
    digest: Hash,
}

pub struct MerkleTree<M: Merklable> {
    root: Option<Rc<RefCell<Branch<M>>>>,
    leaves: HashMap<M::ID, Rc<RefCell<Leaf<M>>>>,
}

// smuggle Rc's across threads
struct Ratn<T> {
    inner: Rc<RefCell<T>>,
}

impl<T> Ratn<T> {
    fn new(inner: Rc<RefCell<T>>) -> Self {
        Self { inner }
    }

    fn into_inner(self) -> Rc<RefCell<T>> {
        self.inner
    }
}

// Safety: For this problem, threads do not ever use the same Rc at the same time
// Thus, we are okay making them Send
unsafe impl<T> Send for Ratn<T> {}

impl<M: Merklable> MerkleTree<M> {
    pub fn new() -> Self {
        MerkleTree {
            root: None,
            leaves: HashMap::new(),
        }
    }

    pub fn digest(&self) -> Hash {
        match self.root {
            Some(ref r) => r.borrow().hash,
            None => blake3::hash(&[]),
        }
    }

    // will need to make multiple updates good
    // for now assume this will be called only once
    pub fn update(&mut self, arr: impl AsRef<[M]>) {
        let arr = arr.as_ref();
        let (leaves_s, leaves_r) = channel::bounded(arr.len());
        let root = Self::generate_subtree::<join::SerialJoin>(arr, leaves_s);
        self.root = Some(root.into_inner());
        self.leaves = leaves_r
            .iter()
            .map(|ratn| ratn.into_inner())
            .map(|leaf| {
                let id = leaf.borrow().inner.id();
                (id, leaf)
            })
            .collect();
    }

    #[cfg(feature = "rayon")]
    pub fn update_rayon(&mut self, arr: impl AsRef<[M]>) {
        let arr = arr.as_ref();
        let (leaves_s, leaves_r) = channel::bounded(arr.len());
        let root = Self::generate_subtree::<join::RayonJoin>(arr, leaves_s);
        self.root = Some(root.into_inner());
        self.leaves = leaves_r
            .iter()
            .map(|leaf| {
                let leaf = leaf.into_inner();
                let id = leaf.borrow().inner.id();
                (id, leaf)
            })
            .collect();
    }

    fn generate_subtree<J: join::Join>(
        arr: &[M],
        leaves_s: Sender<Ratn<Leaf<M>>>,
    ) -> Ratn<Branch<M>> {
        if arr.len() == 2 {
            let inner_l = arr[0];
            let inner_r = arr[1];
            let inner_b = M::Branch::fold(&inner_l.to_branch(), &inner_r.to_branch());

            let hash_l = blake3::hash(&bincode::serialize(&inner_l).unwrap());
            let hash_r = blake3::hash(&bincode::serialize(&inner_r).unwrap());

            let hash_b = {
                let mut hasher = Hasher::new();
                hasher.update(&bincode::serialize(&inner_b).unwrap());
                hasher.update(hash_l.as_bytes());
                hasher.update(hash_r.as_bytes());
                hasher.finalize()
            };

            // Safety: we set the correct parent right after
            let null = unsafe { Rc::from_raw((&hash_b as *const Hash).cast()) }; // this is comically stupid

            let leaf_l = Rc::new(RefCell::new(Leaf::new(inner_l, hash_l, Rc::clone(&null))));
            let leaf_r = Rc::new(RefCell::new(Leaf::new(inner_r, hash_r, Rc::clone(&null))));

            let branch = Rc::new(RefCell::new(Branch::new(
                inner_b,
                hash_b,
                None,
                Child::Leaf(Rc::clone(&leaf_l)),
                Some(Child::Leaf(Rc::clone(&leaf_r))),
            )));

            leaf_l.borrow_mut().set_parent(Rc::clone(&branch));
            leaf_r.borrow_mut().set_parent(Rc::clone(&branch));

            leaves_s.send(Ratn::new(leaf_l)).unwrap();
            leaves_s.send(Ratn::new(leaf_r)).unwrap();

            return Ratn::new(branch);
        }

        if arr.len() == 1 {
            let inner_l = arr[0];
            let inner_b = inner_l.to_branch();
            let hash_l = blake3::hash(&bincode::serialize(&inner_l).unwrap());

            let hash_b = {
                let mut hasher = Hasher::new();
                hasher.update(&bincode::serialize(&inner_b).unwrap());
                hasher.update(hash_l.as_bytes());
                hasher.finalize()
            };

            // Safety: we set the correct parent right after
            let null = unsafe { Rc::from_raw((&hash_b as *const Hash).cast()) }; // this is comically stupid

            let leaf = Rc::new(RefCell::new(Leaf::new(inner_l, hash_l, null)));

            let branch = Rc::new(RefCell::new(Branch::new(
                inner_b,
                hash_b,
                None,
                Child::Leaf(Rc::clone(&leaf)),
                None,
            )));

            leaf.borrow_mut().set_parent(Rc::clone(&branch));

            leaves_s.send(Ratn::new(leaf)).unwrap();

            return Ratn::new(branch);
        }

        const POINTER_WIDTH: usize = mem::size_of::<usize>() * 8;
        let height = POINTER_WIDTH - arr.len().leading_zeros() as usize;
        let partition = 1 << (height - 2);

        let leaves_s_clone = leaves_s.clone();
        let (left, right) = J::join(
            || Self::generate_subtree::<J>(&arr[..partition], leaves_s),
            || Self::generate_subtree::<J>(&arr[partition..], leaves_s_clone),
        );

        let (left, right) = (left.into_inner(), right.into_inner());

        let inner = M::Branch::fold(&left.borrow().inner, &right.borrow().inner);

        let hash = {
            let mut hasher = Hasher::new();
            hasher.update(&bincode::serialize(&inner).unwrap());
            hasher.update(left.borrow().hash.as_bytes());
            hasher.update(right.borrow().hash.as_bytes());
            hasher.finalize()
        };

        let branch = Rc::new(RefCell::new(Branch::new(
            inner,
            hash,
            None,
            Child::Branch(Rc::clone(&left)),
            Some(Child::Branch(Rc::clone(&right))),
        )));

        left.borrow_mut().set_parent(Rc::clone(&branch));
        right.borrow_mut().set_parent(Rc::clone(&branch));

        Ratn::new(branch)
    }

    pub fn generate_proof(&self, id: M::ID) -> Option<Proof<M>> {
        let leaf = self.leaves.get(&id)?;
        let blocks = vec![];

        // find all of the blocks we need to include

        // very first one we cannot guarentee that there is a leaf next or not

        let digest = self.root.as_ref().unwrap().borrow().hash;

        Some(Proof {
            data: leaf.borrow().inner,
            blocks,
            digest,
        })
    }
}

// verify proof

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "rayon")]
    #[test]
    fn test() {
        #[derive(Clone, Copy, Serialize)]
        struct Sum {
            amount: u32,
        }

        impl Branchable for Sum {
            fn fold(l: &Self, r: &Self) -> Self {
                Self {
                    amount: l.amount + r.amount,
                }
            }
        }

        #[derive(Clone, Copy, Serialize)]
        struct Stake {
            id: &'static str,
            amount: u32,
        }

        impl Merklable for Stake {
            type Branch = Sum;
            type ID = &'static str;

            fn to_branch(&self) -> Self::Branch {
                Sum {
                    amount: self.amount,
                }
            }

            fn id(&self) -> Self::ID {
                self.id
            }
        }

        let vals = vec![1; 2_554_432];
        let stakes: Vec<_> = vals
            .iter()
            .map(|v| Stake {
                id: "james",
                amount: *v,
            })
            .collect();

        let mut tree = MerkleTree::new();
        tree.update_rayon(&stakes);

        println!("{:?}", tree.digest());

        panic!();
    }
}
