package kvstore

import (
    "fmt"
    "testing"
    "time"
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
func TestParrll(t *testing.T) {
    tree := NewIntervalTree()
    go func() {
        fmt.Println("xian1")
        tree.lock.Lock()
        for i := 0; i < 10000; i++ {
            interval1,_ := NewInterval("u01","u08")

            tree.Insert(interval1)

        }
        fmt.Println("结束")
        tree.lock.Unlock()
    }()

    go func() {
        time.Sleep(1*time.Second)
        fmt.Println("xian2")

        interval1,_ := NewInterval("u01","u08")
        tree.lock.Lock()
        tree.Insert(interval1)
        tree.lock.Unlock()
        fmt.Println("xiancheng2")
    }()
    time.Sleep(300*time.Second)
}

func TestFindOverlapMultipleReturned1(t *testing.T) {
    tree := NewIntervalTree()

    start := "u01"
    end := "u03"
    interval, _ := NewFileInterval(start, end,1)

    start2 := "u02"
    end2 := "u03"
    interval2, _ := NewFileInterval(start2, end2,2)

    start3 := "u06"
    end3 := "u07"
    interval3, _ := NewFileInterval(start3, end3,3)

    start4 := "u00"
    end4 := "u08"
    interval4, _ := NewFileInterval(start4, end4,4)

    start5 := "u00"
    end5 := "u01"
    interval5, _ := NewFileInterval(start5, end5,5)

    start26 := "u00"
    end26 := "u26"
    interval26, _ := NewFileInterval(start26, end26,26)

    start36 := "u011"
    end36 := "u36"
    interval36, _ := NewFileInterval(start36, end36,36)

    start6 := "u00"
    end6 := "u016"
    interval6, _ := NewFileInterval(start6, end6,6)

    start7 := "u00"
    end7 := "u017"
    interval7, _ := NewFileInterval(start7, end7,7)

    start8 := "u00"
    end8 := "u018"
    interval8, _ := NewFileInterval(start8, end8,8)

    start9 := "u00"
    end9 := "u019"
    interval9, _ := NewFileInterval(start9, end9,9)

    start10 := "u00"
    end10 := "u010"
    interval10, _ := NewFileInterval(start10, end10,10)

    start11 := "u00"
    end11 := "u011"
    interval11, _ := NewFileInterval(start11, end11,11)

    start12 := "u00"
    end12 := "u012"
    interval12, _ := NewFileInterval(start12, end12,12)

    start14 := "u00"
    end14 := "u015"
    interval14, _ := NewFileInterval(start14, end14,14)

    start17 := "u00"
    end17 := "u017"
    interval17, _ := NewFileInterval(start17, end17,17)

    start18 := "u00"
    end18 := "u018"
    interva18, _ := NewFileInterval(start18, end18,18)


    tree.Insert(interval)
    tree.Insert(interval2)
    tree.Insert(interval3)
    tree.Insert(interval4)
    tree.Insert(interval5)
    tree.Insert(interval26)
    tree.Insert(interval36)
    tree.Insert(interval6)
    tree.Insert(interval7)
    tree.Insert(interval8)
    tree.Insert(interval9)
    tree.Insert(interval10)
    tree.Insert(interval11)
    tree.Insert(interval12)
    tree.Insert(interval14)
    tree.Insert(interval17)
    tree.Insert(interva18)

    start111 := "u01"
    end111 := "u01"
    testInterval1, _ := NewInterval(start111, end111)

    overlaps := tree.FindOverlap(testInterval1)
    fmt.Println(overlaps)
}

func TestTwoFang(t *testing.T) {
    arr := make([]int,0)
    T := 10001
    for i := 0; i < T; i++ {
        arr = append(arr, i)
    }
    ans := make([]int,0)
    // 查找
    startT := time.Now()
    for i := 0; i < len(arr); i++ {
        ans = append(ans, arr[i])
    }
    endT := time.Now()

    fmt.Println(endT.Sub(startT).Nanoseconds())

    ans2 := make([]int,0)
    startT2 := time.Now()
    // 一半查找
    for i := 0; i < len(arr)/2; i++ {
        ans2 = append(ans2, arr[i])
    }
    endT2 := time.Now()
    fmt.Println(endT2.Sub(startT2).Nanoseconds())

}