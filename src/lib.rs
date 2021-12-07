mod join;

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash;
use std::mem;
use std::rc::Rc;

use blake3::{Hash, Hasher};
use crossbeam::channel::{self, Sender};
use serde::{Deserialize, Serialize};

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
    index: usize, // keep index here. not sure if this is ideal
    parent: Rc<RefCell<Branch<M>>>,
}

impl<M: Merklable> Leaf<M> {
    fn new(inner: M, hash: Hash, index: usize, parent: Rc<RefCell<Branch<M>>>) -> Self {
        Leaf {
            inner,
            hash,
            index,
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

impl<M: Merklable> Child<M> {
    fn inner(&self) -> M::Branch {
        match self {
            Child::Branch(b) => b.borrow().inner,
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

    pub fn root(&self) -> Option<M::Branch> {
        self.root.as_ref().map(|root| root.borrow().inner)
    }

    // will need to make multiple updates good
    // for now assume this will be called only once
    pub fn update(&mut self, arr: impl AsRef<[M]>) {
        let arr = arr.as_ref();
        let arr: Vec<_> = (0..arr.len()).map(|i| (arr[i], i)).collect();
        let (leaves_s, leaves_r) = channel::bounded(arr.len());
        let root = Self::generate_subtree::<join::SerialJoin>(&arr, leaves_s);
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
        arr: &[(M, usize)],
        leaves_s: Sender<Ratn<Leaf<M>>>,
    ) -> Ratn<Branch<M>> {
        if arr.len() == 2 {
            let index_l = arr[0].1;
            let index_r = arr[1].1;

            let inner_l = arr[0].0;
            let inner_r = arr[1].0;
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

            let leaf_l = Rc::new(RefCell::new(Leaf::new(
                inner_l,
                hash_l,
                index_l,
                Rc::clone(&null),
            )));
            let leaf_r = Rc::new(RefCell::new(Leaf::new(
                inner_r,
                hash_r,
                index_r,
                Rc::clone(&null),
            )));

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
            let index = arr[0].1;

            let inner_l = arr[0].0;
            let inner_b = inner_l.to_branch();
            let hash = blake3::hash(&bincode::serialize(&inner_l).unwrap());

            // let hash_b = {
            //     let mut hasher = Hasher::new();
            //     hasher.update(&bincode::serialize(&inner_b).unwrap());
            //     hasher.update(hash_l.as_bytes());
            //     hasher.finalize()
            // };

            // Safety: we set the correct parent right after
            let null = unsafe { Rc::from_raw((&hash as *const Hash).cast()) }; // this is comically stupid

            let leaf = Rc::new(RefCell::new(Leaf::new(inner_l, hash, index, null)));

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
        let leaf = self.leaves.get(&id)?.borrow();
        let mut index = leaf.index;
        let parent = leaf.parent.borrow();

        // if the first branch only has one child, we skip it
        let mut curr = if parent.right_child.is_none() {
            parent.parent.as_ref().map(|p| Rc::clone(&p))
        } else {
            Some(Rc::clone(&leaf.parent))
        };

        let mut siblings = vec![];

        while let Some(branch) = curr {
            let branch = branch.borrow();

            // need to actually push the other child hmmmm
            let silbing_side = Side::from_int(index & 1).other_side();

            let sibling = match silbing_side {
                Side::Left => &branch.left_child,
                Side::Right => branch.right_child.as_ref().unwrap(),
            };

            let data = sibling.inner();
            let hash = sibling.hash();

            siblings.push(Sibling {
                data,
                side: silbing_side,
                hash,
            });

            // mutate for next iteration
            curr = branch.parent.as_ref().map(|p| Rc::clone(&p));
            index >>= 1;
        }

        Some(Proof {
            data: leaf.inner,
            siblings,
        })
    }
}

pub struct Proof<M: Merklable> {
    data: M,
    siblings: Vec<Sibling<M>>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
enum Side {
    Left,
    Right,
}

impl Side {
    fn other_side(self) -> Self {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }

    // use from primitive later?
    fn from_int(i: usize) -> Self {
        match i {
            0 => Side::Left,
            _ => Side::Right,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Sibling<M: Merklable> {
    data: M::Branch,
    side: Side,
    #[serde(with = "HashDef")]
    hash: Hash,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Hash")]
struct HashDef {
    #[serde(getter = "Hash::as_bytes")]
    bytes: [u8; 32],
}

impl From<HashDef> for Hash {
    fn from(hash: HashDef) -> Hash {
        Hash::from_hex(hash.bytes).unwrap()
    }
}

pub fn verify_proof<M: Merklable>(proof: &Proof<M>, digest: Hash) -> Result<(), ()> {
    // TODO: make more interactive errors

    let mut curr_hash = blake3::hash(&bincode::serialize(&proof.data).unwrap());
    let mut curr_data = proof.data.to_branch();

    for sibling in proof.siblings.iter() {
        curr_data = M::Branch::fold(&curr_data, &sibling.data);
        curr_hash = {
            let mut hasher = Hasher::new();
            hasher.update(&bincode::serialize(&curr_data).unwrap());
            match sibling.side {
                Side::Left => {
                    hasher.update(sibling.hash.as_bytes());
                    hasher.update(curr_hash.as_bytes());
                }
                Side::Right => {
                    hasher.update(curr_hash.as_bytes());
                    hasher.update(sibling.hash.as_bytes());
                }
            }
            hasher.finalize()
        }
    }

    if curr_hash == digest {
        Ok(())
    } else {
        Err(())
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
