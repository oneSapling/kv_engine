package kvstore

import (
    "fmt"
    "testing"
)

func TestTreeCreation(t *testing.T) {
    var tree = NewIntervalTree()
    if tree == nil {
        t.Error("NewIntervalTree should return a non-null pointer to a tree.")
    }
}

func TestInsertEmptyTree(t *testing.T) {
    tree := NewIntervalTree()
    start := "u123"
    end := "u124"
    interval, _ := NewInterval(start, end)

    tree.Insert(interval)

    if tree.root == nil || tree.root.i != interval {
        t.Error("IntervalTree root should contain interval.")
    } else if tree.root.left != nil || tree.root.right != nil {
        t.Error("IntervalTree root should be only node in tree.")
    } else if tree.root.subTreeMax != end {
        t.Error("IntervalTree root has incorrect subTreeMax.")
    }
}

func TestInsertLeftOfRoot(t *testing.T) {
    // insert an interval to left of root (root only node in tree)
    tree := NewIntervalTree()

    // root node
    start := "u01"
    end := "u02"
    interval, _ := NewInterval(start, end)
    tree.Insert(interval)

    // left node
    start2 := "u01"
    end2 := "u02"
    interval2, _ := NewInterval(start2, end2)
    tree.Insert(interval2)

    testNode := newIntervalTreeNode(interval2)

    if tree.root.left == nil || *tree.root.left != *testNode {
        t.Error("Node was not inserted correctly.")
    }
}

func TestInsertRightOfRoot(t *testing.T) {
    // insert an interval to right of root (root only node in tree)
    tree := NewIntervalTree()

    // root node
    start := "u01"
    end := "u02"
    interval, _ := NewInterval(start, end)
    tree.Insert(interval)

    // left node
    start2 := "u01"
    end2 := "u01"
    interval2, _ := NewInterval(start2, end2)
    tree.Insert(interval2)

    testNode := newIntervalTreeNode(interval2)

    if tree.root.right == nil || *tree.root.right != *testNode {
        t.Error("Node was not inserted correctly.")
    }
}

func TestInsertOverlappingRootStart(t *testing.T) {
    // insert an interval that overlaps start of root (root only node in tree)
    // should be inserted to the right (based on insertion algorithm)
    tree := NewIntervalTree()

    // root node
    start := "u01"
    end := "u01"
    interval, _ := NewInterval(start, end)
    tree.Insert(interval)

    // left node
    start2 := "u01"
    end2 := "u01"
    interval2, _ := NewInterval(start2, end2)
    tree.Insert(interval2)

    testNode := newIntervalTreeNode(interval2)

    if tree.root.right == nil || *tree.root.right != *testNode {
        t.Error("Node was not inserted correctly.")
    }
}

func TestInsertOverlappingRootEnd(t *testing.T) {
    // insert an interval that overlaps end of root (root only node in tree)
    // should be inserted to the right (based on insertion algorithm)
    tree := NewIntervalTree()

    // root node
    start := "u01"
    end := "u02"
    interval, _ := NewInterval(start, end)
    tree.Insert(interval)

    // left node
    start2 := "u02"
    end2 := "u04"
    interval2, _ := NewInterval(start2, end2)
    tree.Insert(interval2)

    testNode := newIntervalTreeNode(interval2)

    if tree.root.right == nil || *tree.root.right != *testNode {
        t.Error("Node was not inserted correctly.")
    }
}

func TestInsertWithinRoot(t *testing.T) {
    // insert an interval that is encapsulated by root (root only node in tree)
    // should be inserted to the right (based on insertion algorithm)
    tree := NewIntervalTree()

    // root node
    start := "u01"
    end := "u01"
    interval, _ := NewInterval(start, end)
    tree.Insert(interval)

    // left node
    start2 := "u01"
    end2 := "u01"
    interval2, _ := NewInterval(start2, end2)
    tree.Insert(interval2)

    testNode := newIntervalTreeNode(interval2)

    if tree.root.right == nil || *tree.root.right != *testNode {
        t.Error("Node was not inserted correctly.")
    }
}

func TestInsertEncapsulatingRoot(t *testing.T) {
    // insert an interval that encapsulates root (root only node in tree)
    // should be inserted to the right (based on insertion algorithm)
    tree := NewIntervalTree()

    // root node
    start := "u01"
    end := "u01"
    interval, _ := NewInterval(start, end)
    tree.Insert(interval)

    // left node
    start2 := "u01"
    end2 := "u01"
    interval2, _ := NewInterval(start2, end2)
    tree.Insert(interval2)

    testNode := newIntervalTreeNode(interval2)

    if tree.root.right == nil || *tree.root.right != *testNode {
        t.Error("Node was not inserted correctly.")
    }
}

func TestOverlapsEmptyTree(t *testing.T) {
    tree := NewIntervalTree()

    start := "u01"
    end := "u01"
    interval, _ := NewInterval(start, end)

    if tree.Overlaps(interval) {
        t.Error("Tree should not detect overlap when empty.")
    }
}

func TestOverlapsPopulatedTree(t *testing.T) {
    tree := NewIntervalTree()

    start := "u01"
    end := "u02"
    interval, _ := NewInterval(start, end)

    start2 := "u03"
    end2 := "u04"
    interval2, _ := NewInterval(start2, end2)

    start3 := "u01"
    end3 := "u05"
    interval3, _ := NewInterval(start3, end3)

    start4 := "u06"
    end4 := "u07"
    interval4, _ := NewInterval(start4, end4)

    tree.Insert(interval)
    tree.Insert(interval2)
    tree.Insert(interval3)
    tree.Insert(interval4)

    // overlaps nothing
    start5 := "u08"
    end5 := "u09"
    testInterval1, _ := NewInterval(start5, end5)

    // overlaps all intervals
    start6 := "u01"
    end6 := "u03"
    testInterval2, _ := NewInterval(start6, end6)

    if tree.Overlaps(testInterval1) {
        fmt.Println("区间一有重合")
    }

    if tree.Overlaps(testInterval2) {
        fmt.Println("区间二有重合")
    }

    interval7, _ := NewInterval("u01", "u02", )
    overlaps := tree.FindOverlap(interval7)
    fmt.Println(overlaps)
}

func TestFindOverlapEmptyTree(t *testing.T) {
    tree := NewIntervalTree()
    interval, _ := NewInterval("u01", "u02", )
    overlaps := tree.FindOverlap(interval)
    if len(overlaps) != 0 {
        t.Error("Tree found a phantom overlapping interval.")
    }
}

func TestFindOverlapNothingReturned(t *testing.T) {
    tree := NewIntervalTree()

    start := "u01"
    end := "u01"
    interval, _ := NewInterval(start, end)

    start2 := "u01"
    end2 := "u01"
    interval2, _ := NewInterval(start2, end2)

    start3 := "u01"
    end3 := "u01"
    interval3, _ := NewInterval(start3, end3)

    start4 := "u01"
    end4 := "u01"
    interval4, _ := NewInterval(start4, end4)

    tree.Insert(interval)
    tree.Insert(interval2)
    tree.Insert(interval3)
    tree.Insert(interval4)

    start5 := "u01"
    end5 := "u01"
    testInterval1, _ := NewInterval(start5, end5)

    overlaps := tree.FindOverlap(testInterval1)
    fmt.Println(overlaps)
}

func TestFindOverlapMultipleReturned(t *testing.T) {
    tree := NewIntervalTree()

    start := "u01"
    end := "u01"
    interval, _ := NewInterval(start, end)

    start2 := "u01"
    end2 := "u03"
    interval2, _ := NewInterval(start2, end2)

    start3 := "u05"
    end3 := "u07"
    interval3, _ := NewInterval(start3, end3)

    start4 := "u01"
    end4 := "u08"
    interval4, _ := NewInterval(start4, end4)

    tree.Insert(interval)
    tree.Insert(interval2)
    tree.Insert(interval3)
    tree.Insert(interval4)

    start5 := "u01"
    end5 := "u05"
    testInterval1, _ := NewInterval(start5, end5)

    overlaps := tree.FindOverlap(testInterval1)
    fmt.Println(overlaps)
}

func TestTreeEmpty(t *testing.T) {
    tree := NewIntervalTree()
    if tree.Empty() == false {
        t.Error("NewIntervalTree should return an empty tree.")
    }

    start := "u01"
    end := "u01"
    interval, _ := NewInterval(start, end)
    tree.Insert(interval)

    if tree.Empty() == true {
        t.Error("IntervalTree should not be empty after insertion.")
    }
}
