use crate::SkipList;

#[test]
fn test_find_or_next() {
    let list = SkipList::new(10240, |a, b| a.cmp(b)).unwrap();
    list.push(b"1", b"1").unwrap();
    list.push(b"2", b"2").unwrap();
    list.push(b"3", b"3").unwrap();
    list.push(b"4", b"4").unwrap();
    list.push(b"5", b"5").unwrap();
    list.push(b"6", b"6").unwrap();
    list.push(b"7", b"7").unwrap();
    list.push(b"8", b"8").unwrap();
    list.push(b"9", b"9").unwrap();
    list.push(b"10", b"10").unwrap();
    assert_eq!(
        list.find_or_next(b"1", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"1"
    );
    assert_eq!(
        list.find_or_next(b"2", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"2"
    );
    assert_eq!(
        list.find_or_next(b"3", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"3"
    );
    assert_eq!(
        list.find_or_next(b"4", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"4"
    );
    assert_eq!(
        list.find_or_next(b"5", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"5"
    );
    assert_eq!(
        list.find_or_next(b"6", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"6"
    );
    assert_eq!(
        list.find_or_next(b"7", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"7"
    );
    assert_eq!(
        list.find_or_next(b"8", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"8"
    );
    assert_eq!(
        list.find_or_next(b"9", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"9"
    );
    assert_eq!(
        list.find_or_next(b"10", false)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"10"
    );
    assert_eq!(
        list.find_or_next(b"0", true)
            .unwrap()
            .get_key(&list.arena)
            .unwrap(),
        b"1"
    );
    assert!(list.find_or_next(b"11", false).is_none());
}
#[test]
fn test_find_prev() {
    let list = SkipList::new(10240, |a, b| a.cmp(b)).unwrap();
    list.push(b"1", b"1").unwrap();
    list.push(b"2", b"2").unwrap();
    list.push(b"3", b"3").unwrap();
    list.push(b"4", b"4").unwrap();
    list.push(b"5", b"5").unwrap();
    list.push(b"6", b"6").unwrap();
    list.push(b"7", b"7").unwrap();
    list.push(b"8", b"8").unwrap();
    list.push(b"9", b"9").unwrap();

    assert!(list.find_prev(b"1").is_none());
    assert_eq!(
        list.find_prev(b"2").unwrap().get_key(&list.arena).unwrap(),
        b"1"
    );
    assert_eq!(
        list.find_prev(b"3").unwrap().get_key(&list.arena).unwrap(),
        b"2"
    );
    assert_eq!(
        list.find_prev(b"4").unwrap().get_key(&list.arena).unwrap(),
        b"3"
    );
    assert_eq!(
        list.find_prev(b"5").unwrap().get_key(&list.arena).unwrap(),
        b"4"
    );
    assert_eq!(
        list.find_prev(b"6").unwrap().get_key(&list.arena).unwrap(),
        b"5"
    );
    assert_eq!(
        list.find_prev(b"7").unwrap().get_key(&list.arena).unwrap(),
        b"6"
    );
    assert_eq!(
        list.find_prev(b"8").unwrap().get_key(&list.arena).unwrap(),
        b"7"
    );
    assert_eq!(
        list.find_prev(b"9").unwrap().get_key(&list.arena).unwrap(),
        b"8"
    );
    assert!(list.find_prev(b"0").is_none());
}
#[test]
fn test_find_next() {
    let list = SkipList::new(10240, |a, b| a.cmp(b)).unwrap();
    list.push(b"1", b"1").unwrap();
    list.push(b"2", b"2").unwrap();
    list.push(b"3", b"3").unwrap();
    list.push(b"4", b"4").unwrap();
    list.push(b"5", b"5").unwrap();
    list.push(b"6", b"6").unwrap();
    list.push(b"7", b"7").unwrap();
    list.push(b"8", b"8").unwrap();
    list.push(b"9", b"9").unwrap();
    assert_eq!(
        list.find_next(b"0").unwrap().get_key(&list.arena).unwrap(),
        b"1"
    );
    assert_eq!(
        list.find_next(b"1").unwrap().get_key(&list.arena).unwrap(),
        b"2"
    );
    assert_eq!(
        list.find_next(b"2").unwrap().get_key(&list.arena).unwrap(),
        b"3"
    );
    assert_eq!(
        list.find_next(b"3").unwrap().get_key(&list.arena).unwrap(),
        b"4"
    );
    assert_eq!(
        list.find_next(b"4").unwrap().get_key(&list.arena).unwrap(),
        b"5"
    );
    assert_eq!(
        list.find_next(b"5").unwrap().get_key(&list.arena).unwrap(),
        b"6"
    );
    assert_eq!(
        list.find_next(b"6").unwrap().get_key(&list.arena).unwrap(),
        b"7"
    );
    assert_eq!(
        list.find_next(b"7").unwrap().get_key(&list.arena).unwrap(),
        b"8"
    );
    assert_eq!(
        list.find_next(b"8").unwrap().get_key(&list.arena).unwrap(),
        b"9"
    );
    assert!(list.find_next(b"9").is_err());
}
#[test]
fn test_find_last() {
    let list = SkipList::new(10240, |a, b| a.cmp(b)).unwrap();
    list.push(b"1", b"1").unwrap();
    list.push(b"2", b"2").unwrap();
    list.push(b"3", b"3").unwrap();
    list.push(b"4", b"4").unwrap();
    list.push(b"5", b"5").unwrap();
    list.push(b"6", b"6").unwrap();
    list.push(b"7", b"7").unwrap();
    list.push(b"8", b"8").unwrap();
    list.push(b"9", b"9").unwrap();
    assert_eq!(
        list.find_last().unwrap().get_key(&list.arena).unwrap(),
        b"9"
    );
    let list = SkipList::new(10240, |a, b| a.cmp(b)).unwrap();

    assert!(list.find_last().is_none());
}
