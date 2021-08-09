package kvstore

import (
    "errors"
    "fmt"
)

// t.before（u）判断t是否在u前面
// t.after（u）判断t是否在u前面

// wishlist
// polymorphic intervals with an interface and a method to compare them
// a payload within each interval
// AVL tree or red black tree insertion/deletion instead of BST


/* 
    Base IntervalTree class.
    Entry point for the tree (instead of just holding a pointer to root).
    Use *IntervalTree Receiver Type to avoid copying
 */
type IntervalTree struct {
    root *IntervalTreeNode
}

func NewIntervalTree() *IntervalTree {
    // constructor: create tree (empty)
    return &IntervalTree{nil}
}

func (tree *IntervalTree) Empty() bool {
    // Empty: returns boolean if tree is empty
    return tree.root == nil
}

func (tree *IntervalTree) Insert(i Interval) {
    // Insert: inserts an interval
    if tree.Empty() {
        tree.root = newIntervalTreeNode(i)
    } else {
        tree.root.insert(i)
    }
}

func (tree *IntervalTree) FindOverlap(i Interval) []Interval {
    // FindOverlap: find all intervals overlapping with an interval
    if tree.Empty() {
        var overlaps []Interval
        return overlaps
    } else {
        overlaps := tree.root.findOverlap(i)
        return overlaps
    }
}

func (tree *IntervalTree) Overlaps(i Interval) bool {
    // Overlaps: check if any interval in tree overlaps an interval
    if tree.Empty() {
        return false
    } else {
        return tree.root.overlaps(i)
    }
}

/*
    A node in the interval tree, useful for balanced tree insertion
    without having to contaminate interval class.
 */
type IntervalTreeNode struct {
    i Interval
    subTreeMax string
    left *IntervalTreeNode
    right *IntervalTreeNode
}

func newIntervalTreeNode(i Interval) *IntervalTreeNode {
    // TODO: return error if i is nil
    node := new(IntervalTreeNode)
    node.i = i
    node.subTreeMax = i.End()
    return node
}

func (node *IntervalTreeNode) insert(i Interval) *IntervalTreeNode  {
    start := node.i.Start()
    if UserKeyComparator([]byte(i.End()), []byte(start)) < 0 {
        if node.left == nil {
            node.left = newIntervalTreeNode(i)
        } else {
            node.left.insert(i)
        }
    } else {
        if node.right == nil {
            node.right = newIntervalTreeNode(i)
        } else {
            node.right.insert(i)
        }
    }

    // update max for searching later
    if UserKeyComparator([]byte(node.subTreeMax), []byte(i.End())) < 0 {
        node.subTreeMax = i.End()
    }

    return node
}

func (node *IntervalTreeNode) findOverlap(i Interval) []Interval {
    // TODO: be more efficient with searching, this is just going through every single node
    var overlaps []Interval

    if Overlaps(node.i, i) {
        overlaps = append(overlaps, node.i)
    }

    if node.left != nil {
        overlaps = append(overlaps, node.left.findOverlap(i)...)
    }

    if node.right != nil {
        overlaps = append(overlaps, node.right.findOverlap(i)...)
    }

    return overlaps
}

func (node *IntervalTreeNode) overlaps(i Interval) bool {
    // TODO: be more efficient searching
    if Overlaps(node.i, i) {
        return true
    } else if node.left != nil && node.left.overlaps(i) {
        return true
    } else if node.right != nil && node.right.overlaps(i) {
        return true
    } else {
        return false
    }
}



/*
    Start and End times only. 
    Pass these by value instead of pointer because don't want any 
    unexpected modifications (would destroy integrity of tree) and its safer.
    TODO: polymorphic intervals
    Immutable start and end so that intervals are always valid.
 */
type Interval struct {
    start string
    end string
    // Payload PayLoad
}

func NewInterval(start string, end string) (Interval, error) {
    var i Interval
    if UserKeyComparator([]byte(start), []byte(end)) > 0 {
        fmt.Println("开始的范围大于末尾，这不是一个区间")
        return i, errors.New("Interval::NewInterval end cannot come after start.")
    }else {
        return Interval{start, end}, nil
    }
}

func (i Interval) Start() string {
    return i.start
}

func (i Interval) End() string {
    return i.end
}

// start_datetime1 <= end_datetime2 and end_datetime1 >= start_datetime2
func Overlaps(i1 Interval, i2 Interval) bool {
    return UserKeyComparator([]byte(i1.Start()), []byte(i2.End())) <= 0 && UserKeyComparator([]byte(i1.End()), []byte(i2.Start())) >= 0
}