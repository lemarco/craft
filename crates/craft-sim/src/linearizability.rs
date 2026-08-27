//! A linearizability checker over recorded operation histories (testing-strategy, I5).
//!
//! Given a concurrent history of `(invocation, response)` events and a
//! *sequential specification* ([`Model`]), this decides whether there exists a
//! total order of the operations that (a) respects real-time precedence — if
//! op *A* returned before op *B* was invoked then *A* precedes *B* — and (b) is
//! a legal run of the model (each operation's observed output matches what the
//! model would produce). That is exactly the definition of linearizability
//! (Herlihy & Wing).
//!
//! The algorithm is the Wing–Gong / Lowe DFS used by
//! [`porcupine`](https://github.com/anishathalye/porcupine) and Knossos: keep
//! the events in a doubly linked list, repeatedly try to *lift* the earliest
//! still-pending completed operation into the linearization, recurse, and
//! backtrack on failure. Visited `(state, linearized-set)` configurations are
//! memoized so the search does not re-explore equivalent branches — the pruning
//! that makes it tractable in practice.
//!
//! Scope: histories where every invocation has a matching response (all
//! operations complete). This fits the deterministic simulator's histories,
//! where each proposed write and each `ReadIndex` read runs to completion.

use std::collections::HashSet;
use std::hash::Hash;

/// A deterministic sequential specification the history is checked against.
pub trait Model {
    /// The model's abstract state.
    type State: Clone + Eq + Hash;
    /// The input of an operation (e.g. `Write(v)` / `Read`).
    type Input: Clone;
    /// The observable output of an operation (e.g. the value read).
    type Output: Clone + Eq;

    /// The initial state.
    fn init(&self) -> Self::State;

    /// Apply `input` to `state`, returning the next state and the output the
    /// specification produces. The checker linearizes an operation here only if
    /// this output equals the one actually observed in the history.
    fn apply(&self, state: &Self::State, input: &Self::Input) -> (Self::State, Self::Output);
}

/// A recorded history: a time-ordered sequence of invocation/response events,
/// paired by a per-client (process) id.
///
/// Build it by interleaving [`invoke`](History::invoke) and
/// [`response`](History::response) calls in the order the events occurred.
pub struct History<I, O> {
    events: Vec<Event<I, O>>,
}

enum Event<I, O> {
    Invoke { process: usize, input: I },
    Response { process: usize, output: O },
}

impl<I, O> Default for History<I, O> {
    fn default() -> Self {
        Self { events: Vec::new() }
    }
}

impl<I: Clone, O: Clone + Eq> History<I, O> {
    /// An empty history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `process` invoked an operation with `input`.
    pub fn invoke(&mut self, process: usize, input: I) {
        self.events.push(Event::Invoke { process, input });
    }

    /// Record that `process`'s in-flight operation returned `output`.
    pub fn response(&mut self, process: usize, output: O) {
        self.events.push(Event::Response { process, output });
    }

    /// Number of recorded events (invocations + responses).
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the history has no events.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Whether the history is linearizable against `model`.
    ///
    /// # Panics
    /// Panics if the history is malformed (a response with no matching
    /// invocation, an unmatched invocation, or more than 128 operations).
    #[must_use]
    pub fn is_linearizable<M>(&self, model: &M) -> bool
    where
        M: Model<Input = I, Output = O>,
    {
        Checker::build(self).run(model)
    }
}

/// One node of the linked-list history (an invocation or a response).
struct Node {
    op: usize,
    is_invoke: bool,
    partner: usize,
    prev: usize,
    next: usize,
}

/// Internal WGL search state built from a [`History`].
struct Checker<I, O> {
    nodes: Vec<Node>,
    inputs: Vec<I>,
    outputs: Vec<O>,
    /// Sentinel index (== `nodes.len()`); its `next` is the list head.
    head: usize,
    n_ops: usize,
}

impl<I: Clone, O: Clone + Eq> Checker<I, O> {
    fn build(history: &History<I, O>) -> Self {
        let e = history.events.len();
        // One extra slot for the sentinel node at index `e`.
        let mut nodes: Vec<Node> = Vec::with_capacity(e + 1);
        let mut inputs: Vec<I> = Vec::new();
        let mut outputs: Vec<Option<O>> = Vec::new();
        // process -> (op id, invocation node index) for the in-flight op.
        let mut pending: Vec<Option<(usize, usize)>> = Vec::new();

        for (idx, event) in history.events.iter().enumerate() {
            match event {
                Event::Invoke { process, input } => {
                    let op = inputs.len();
                    inputs.push(input.clone());
                    outputs.push(None);
                    if *process >= pending.len() {
                        pending.resize(process + 1, None);
                    }
                    assert!(
                        pending[*process].is_none(),
                        "process {process} invoked twice without a response"
                    );
                    pending[*process] = Some((op, idx));
                    nodes.push(Node {
                        op,
                        is_invoke: true,
                        partner: usize::MAX,
                        prev: 0,
                        next: 0,
                    });
                }
                Event::Response { process, output } => {
                    let (op, invoke_idx) = pending
                        .get_mut(*process)
                        .and_then(Option::take)
                        .expect("response without a matching invocation");
                    outputs[op] = Some(output.clone());
                    nodes.push(Node {
                        op,
                        is_invoke: false,
                        partner: invoke_idx,
                        prev: 0,
                        next: 0,
                    });
                    nodes[invoke_idx].partner = idx;
                }
            }
        }
        assert!(
            pending.iter().all(Option::is_none),
            "history has an unmatched invocation (all operations must complete)"
        );
        let n_ops = inputs.len();
        assert!(n_ops <= 128, "checker supports at most 128 operations");

        // Sentinel + circular doubly linked list in event order.
        let sentinel = e;
        nodes.push(Node {
            op: usize::MAX,
            is_invoke: false,
            partner: usize::MAX,
            prev: 0,
            next: 0,
        });
        if e == 0 {
            nodes[sentinel].next = sentinel;
            nodes[sentinel].prev = sentinel;
        } else {
            nodes[sentinel].next = 0;
            nodes[0].prev = sentinel;
            for i in 0..e - 1 {
                nodes[i].next = i + 1;
                nodes[i + 1].prev = i;
            }
            nodes[e - 1].next = sentinel;
            nodes[sentinel].prev = e - 1;
        }

        Self {
            nodes,
            inputs,
            outputs: outputs
                .into_iter()
                .map(|o| o.expect("output set"))
                .collect(),
            head: sentinel,
            n_ops,
        }
    }

    /// Remove an invocation node and its matching response from the list.
    fn lift(&mut self, invoke: usize) {
        let response = self.nodes[invoke].partner;
        let (ip, in_) = (self.nodes[invoke].prev, self.nodes[invoke].next);
        self.nodes[ip].next = in_;
        self.nodes[in_].prev = ip;
        let (rp, rn) = (self.nodes[response].prev, self.nodes[response].next);
        self.nodes[rp].next = rn;
        self.nodes[rn].prev = rp;
    }

    /// Reinsert a previously [`lift`](Self::lift)ed invocation and its response
    /// (nodes retain their own `prev`/`next`, so we restore neighbor links).
    fn unlift(&mut self, invoke: usize) {
        let response = self.nodes[invoke].partner;
        let (rp, rn) = (self.nodes[response].prev, self.nodes[response].next);
        self.nodes[rp].next = response;
        self.nodes[rn].prev = response;
        let (ip, in_) = (self.nodes[invoke].prev, self.nodes[invoke].next);
        self.nodes[ip].next = invoke;
        self.nodes[in_].prev = invoke;
    }

    fn run<M>(&mut self, model: &M) -> bool
    where
        M: Model<Input = I, Output = O>,
    {
        let mut state = model.init();
        let mut linearized: u128 = 0;
        let mut calls: Vec<(usize, M::State)> = Vec::new();
        let mut cache: HashSet<(u128, M::State)> = HashSet::new();
        let mut entry = self.nodes[self.head].next;

        while self.nodes[self.head].next != self.head {
            if entry == self.head {
                // Reached the end without emptying the list: backtrack.
                let Some((prev_entry, saved)) = calls.pop() else {
                    return false;
                };
                linearized &= !(1u128 << self.nodes[prev_entry].op);
                state = saved;
                self.unlift(prev_entry);
                entry = self.nodes[prev_entry].next;
                continue;
            }

            if self.nodes[entry].is_invoke {
                let op = self.nodes[entry].op;
                let (next_state, produced) = model.apply(&state, &self.inputs[op]);
                if produced == self.outputs[op] {
                    let new_linearized = linearized | (1u128 << op);
                    if cache.insert((new_linearized, next_state.clone())) {
                        calls.push((entry, state.clone()));
                        linearized = new_linearized;
                        state = next_state;
                        self.lift(entry);
                        entry = self.nodes[self.head].next;
                    } else {
                        // Equivalent configuration already explored — skip.
                        entry = self.nodes[entry].next;
                    }
                } else {
                    entry = self.nodes[entry].next;
                }
            } else {
                // A response whose invocation is still pending to our left: this
                // prefix cannot be completed, so backtrack.
                let Some((prev_entry, saved)) = calls.pop() else {
                    return false;
                };
                linearized &= !(1u128 << self.nodes[prev_entry].op);
                state = saved;
                self.unlift(prev_entry);
                entry = self.nodes[prev_entry].next;
            }
        }
        let _ = self.n_ops;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single integer register: `Write(v)` sets and echoes `v`; `Read`
    /// returns the current value.
    #[derive(Clone)]
    enum Op {
        Write(u64),
        Read,
    }
    struct Register;
    impl Model for Register {
        type State = u64;
        type Input = Op;
        type Output = u64;
        fn init(&self) -> u64 {
            0
        }
        fn apply(&self, state: &u64, input: &Op) -> (u64, u64) {
            match input {
                Op::Write(v) => (*v, *v),
                Op::Read => (*state, *state),
            }
        }
    }

    #[test]
    fn sequential_write_then_read_is_linearizable() {
        let mut h = History::new();
        h.invoke(0, Op::Write(1));
        h.response(0, 1);
        h.invoke(0, Op::Read);
        h.response(0, 1);
        assert!(h.is_linearizable(&Register));
    }

    #[test]
    fn read_after_write_returning_stale_value_is_not_linearizable() {
        // W(1) fully completes, then W(2) fully completes, then a Read returns 1.
        let mut h = History::new();
        h.invoke(0, Op::Write(1));
        h.response(0, 1);
        h.invoke(0, Op::Write(2));
        h.response(0, 2);
        h.invoke(0, Op::Read);
        h.response(0, 1); // should be 2
        assert!(!h.is_linearizable(&Register));
    }

    #[test]
    fn concurrent_read_may_observe_either_order() {
        // Read overlaps Write(1); returning the new value is linearizable
        // (Write linearized before the Read).
        let mut h = History::new();
        h.invoke(0, Op::Write(1)); // P0 starts write
        h.invoke(1, Op::Read); // P1 starts read (concurrent)
        h.response(1, 1); // read sees 1
        h.response(0, 1); // write acks
        assert!(h.is_linearizable(&Register));

        // The same overlap where the read sees the *old* value is also legal.
        let mut h = History::new();
        h.invoke(0, Op::Write(1));
        h.invoke(1, Op::Read);
        h.response(1, 0); // read sees 0 (before the write)
        h.response(0, 1);
        assert!(h.is_linearizable(&Register));
    }

    #[test]
    fn reading_a_never_written_value_is_not_linearizable() {
        let mut h = History::new();
        h.invoke(0, Op::Write(1));
        h.response(0, 1);
        h.invoke(0, Op::Read);
        h.response(0, 5); // 5 was never written
        assert!(!h.is_linearizable(&Register));
    }

    #[test]
    fn two_clients_ordered_by_real_time_must_agree() {
        // P0 writes 7 and completes; only then P1 reads. The read must see 7.
        let mut h = History::new();
        h.invoke(0, Op::Write(7));
        h.response(0, 7);
        h.invoke(1, Op::Read);
        h.response(1, 0); // stale — impossible given real-time order
        assert!(!h.is_linearizable(&Register));
    }

    #[test]
    fn classic_non_linearizable_overlapping_reads() {
        // Two reads overlap each other and a write; the earlier-*returning* read
        // sees the new value while the later-returning one sees the old value.
        // Because the reads overlap, they can be reordered — so this IS
        // linearizable: order R2(0) < W(1) < R1(1).
        let mut h = History::new();
        h.invoke(0, Op::Write(1));
        h.invoke(1, Op::Read); // R1
        h.invoke(2, Op::Read); // R2
        h.response(1, 1); // R1 -> 1
        h.response(2, 0); // R2 -> 0
        h.response(0, 1);
        assert!(h.is_linearizable(&Register));
    }

    #[test]
    fn empty_history_is_linearizable() {
        let h: History<Op, u64> = History::new();
        assert!(h.is_linearizable(&Register));
    }
}
