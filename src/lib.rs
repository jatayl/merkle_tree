mod join;
mod util;

use util::Ratn;

use std::cell::RefCell;
use std::collections::HashMap;
use std::default::Default;
use std::hash;
use std::rc::Rc;

use blake3::{Hash, Hasher};
use crossbeam::channel::{self, Sender};
use serde::{Deserialize, Serialize};

pub trait Branchable: Clone + Serialize {
    fn fold(l: &Self, r: &Self) -> Self;
}

pub trait Merklable: Sync + Clone + Serialize {
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
struct Leaf<M: Merklable> {
    inner: M,
    hash: Hash,
    index: usize,
    // TODO: i want to get rid of this option wrapper without introducing UB
    parent: Option<Rc<RefCell<Branch<M>>>>,
}

impl<M: Merklable> Leaf<M> {
    fn new(inner: M, hash: Hash, index: usize, parent: Option<Rc<RefCell<Branch<M>>>>) -> Self {
        Leaf {
            inner,
            hash,
            index,
            parent,
        }
    }

    fn set_parent(&mut self, parent: Rc<RefCell<Branch<M>>>) {
        self.parent = Some(parent);
    }
}

enum Child<M: Merklable> {
    Branch(Rc<RefCell<Branch<M>>>),
    Leaf(Rc<RefCell<Leaf<M>>>),
}

impl<M: Merklable> Child<M> {
    fn inner(&self) -> M::Branch {
        match self {
            Child::Branch(b) => b.borrow().inner.clone(),
            Child::Leaf(l) => l.borrow().inner.to_branch(),
        }
    }

    fn hash(&self) -> Hash {
        match self {
            Child::Branch(b) => b.borrow().hash,
            Child::Leaf(l) => l.borrow().hash,
        }
    }
}

pub struct MerkleTree<M: Merklable> {
    root: Option<Rc<RefCell<Branch<M>>>>,
    leaves: HashMap<M::ID, Rc<RefCell<Leaf<M>>>>,
}

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

    pub fn root(&self) -> Option<M::Branch> {
        self.root.as_ref().map(|root| root.borrow().inner.clone())
    }

    // will need to make multiple updates good
    // for now assume this will be called only once
    pub fn update(&mut self, arr: impl AsRef<[M]>) {
        self.update_helper::<join::SerialJoin>(arr.as_ref())
    }

    #[cfg(feature = "rayon")]
    pub fn update_rayon(&mut self, arr: impl AsRef<[M]>) {
        self.update_helper::<join::RayonJoin>(arr.as_ref())
    }

    fn update_helper<J: join::Join>(&mut self, arr: &[M]) {
        let arr: Vec<_> = (0..arr.len()).map(|i| (arr[i].clone(), i)).collect();
        let (leaves_s, leaves_r) = channel::bounded(arr.len());
        let root = Self::generate_subtree::<J>(&arr, leaves_s);
        self.root = Some(unsafe { root.into_inner() });
        self.leaves = leaves_r
            .iter()
            .map(|leaf| {
                let leaf = unsafe { leaf.into_inner() };
                let id = leaf.borrow().inner.id();
                (id, leaf)
            })
            .collect();
        assert_eq!(self.leaves.len(), arr.len());
    }

    fn generate_subtree<J: join::Join>(
        arr: &[(M, usize)],
        leaves_s: Sender<Ratn<Leaf<M>>>,
    ) -> Ratn<Branch<M>> {
        if arr.len() == 2 {
            let (inner_l, index_l) = arr[0].clone();
            let (inner_r, index_r) = arr[1].clone();

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

            let leaf_l = Rc::new(RefCell::new(Leaf::new(inner_l, hash_l, index_l, None)));
            let leaf_r = Rc::new(RefCell::new(Leaf::new(inner_r, hash_r, index_r, None)));

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
            let (inner_l, index) = arr[0].clone();

            let inner_b = inner_l.to_branch();
            let hash = blake3::hash(&bincode::serialize(&inner_l).unwrap());

            let leaf = Rc::new(RefCell::new(Leaf::new(inner_l, hash, index, None)));

            let branch = Rc::new(RefCell::new(Branch::new(
                inner_b,
                hash,
                None,
                Child::Leaf(Rc::clone(&leaf)),
                None,
            )));

            leaf.borrow_mut().set_parent(Rc::clone(&branch));

            leaves_s.send(Ratn::new(leaf)).unwrap();

            return Ratn::new(branch);
        }

        let partition = util::partition(arr.len());

        let leaves_s_clone = leaves_s.clone();
        let (left, right) = J::join(
            || Self::generate_subtree::<J>(&arr[..partition], leaves_s),
            || Self::generate_subtree::<J>(&arr[partition..], leaves_s_clone),
        );

        let (left, right) = unsafe { (left.into_inner(), right.into_inner()) };

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
        let leaf = self.leaves.get(&id)?.borrow();
        let parent = leaf.parent.as_ref().unwrap().borrow();

        // if the first branch only has one child, we skip it
        let mut curr = if parent.right_child.is_none() {
            parent.parent.as_ref().map(|p| Rc::clone(p))
        } else {
            Some(Rc::clone(leaf.parent.as_ref().unwrap()))
        };

        // not sure if this is best way of dealing with problem
        // but if the leaf has no sibling then it has no valid step
        // in path to root so we remove it
        let path_to_root = if parent.right_child.is_none() {
            util::path_to_root(leaf.index, self.count()).0 >> 1
        } else {
            util::path_to_root(leaf.index, self.count()).0
        };

        let mut path_to_root_mut = path_to_root;

        let mut siblings = vec![];

        while let Some(branch) = curr {
            let branch = branch.borrow();

            let sibling = match path_to_root_mut & 1 {
                0 => branch.right_child.as_ref().unwrap(),
                _ => &branch.left_child,
            };

            let data = sibling.inner().clone();
            let hash = sibling.hash();

            siblings.push(Sibling { data, hash });

            // mutate for next iteration
            curr = branch.parent.as_ref().map(|p| Rc::clone(p));
            path_to_root_mut >>= 1;
        }

        Some(Proof {
            data: leaf.inner.clone(),
            index: leaf.index,
            path_to_root,
            siblings,
        })
    }

    pub fn count(&self) -> usize {
        self.leaves.len()
    }

    pub fn height(&self) -> usize {
        util::log2(self.count())
    }
}

impl<M: Merklable> Default for MerkleTree<M> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Proof<M: Merklable> {
    data: M,
    index: usize,
    path_to_root: usize,
    siblings: Vec<Sibling<M>>,
}

impl<M: Merklable> Proof<M> {
    pub fn data(&self) -> &M {
        &self.data
    }

    pub fn index(&self) -> usize {
        self.index
    }
}

#[derive(Serialize, Deserialize)]
struct Sibling<M: Merklable> {
    data: M::Branch,
    #[serde(with = "util::HashDef")]
    hash: Hash,
}

pub fn verify_proof<M: Merklable>(proof: &Proof<M>, digest: Hash) -> bool {
    // TODO: make interactive errors

    let mut curr_hash = blake3::hash(&bincode::serialize(&proof.data).unwrap());

    let mut curr_data = proof.data.to_branch();
    let mut path_to_root = proof.path_to_root;

    for sibling in proof.siblings.iter() {
        curr_data = M::Branch::fold(&curr_data, &sibling.data);
        curr_hash = {
            let mut hasher = Hasher::new();
            hasher.update(&bincode::serialize(&curr_data).unwrap());
            match path_to_root & 1 {
                0 => {
                    hasher.update(curr_hash.as_bytes());
                    hasher.update(sibling.hash.as_bytes());
                }
                _ => {
                    hasher.update(sibling.hash.as_bytes());
                    hasher.update(curr_hash.as_bytes());
                }
            }
            hasher.finalize()
        };
        path_to_root >>= 1;
    }

    curr_hash == digest
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    struct Sum(u32);

    impl Branchable for Sum {
        fn fold(l: &Self, r: &Self) -> Self {
            Self(l.0 + r.0)
        }
    }

    #[derive(Debug, Clone, Serialize)]
    struct Stake {
        id: u32,
        amount: u32,
    }

    impl Merklable for Stake {
        type Branch = Sum;
        type ID = u32;

        fn to_branch(&self) -> Self::Branch {
            Sum(self.amount)
        }

        fn id(&self) -> Self::ID {
            self.id
        }
    }

    #[test]
    fn fold() {
        let stakes: Vec<_> = (0..20_001).map(|i| Stake { id: i, amount: 1 }).collect();
        let mut tree = MerkleTree::new();
        tree.update(&stakes);
        assert_eq!(tree.root(), Some(Sum(20_001)));

        let stakes: Vec<_> = (0..12_341).map(|i| Stake { id: i, amount: 1 }).collect();
        let mut tree = MerkleTree::new();
        tree.update(&stakes);
        assert_eq!(tree.root(), Some(Sum(12_341)));
    }

    #[test]
    fn proof() {
        let stakes: Vec<_> = (0..20_001).map(|i| Stake { id: i, amount: 1 }).collect();
        let mut tree = MerkleTree::new();
        tree.update(&stakes);

        for i in 0..20_001 {
            let proof = tree.generate_proof(i).unwrap();
            assert!(verify_proof(&proof, tree.digest()), "{}", i);
        }
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn rayon() {
        let stakes: Vec<_> = (0..50_000).map(|i| Stake { id: i, amount: 1 }).collect();
        let mut tree = MerkleTree::new();
        tree.update_rayon(&stakes);
        let digest_rayon = tree.digest();

        let mut tree = MerkleTree::new();
        tree.update(&stakes);
        let digest_serial = tree.digest();

        assert_eq!(digest_rayon, digest_serial);
    }
}
