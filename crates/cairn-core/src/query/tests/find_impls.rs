use crate::anchor::AnchorName;
use crate::cas::store;
use crate::query::{FindSubtypesArgs, FindSupertypesArgs, find_subtypes, find_supertypes};
use crate::register::register_repo;
use crate::testutil::init_repo;

#[test]
fn find_subtypes_includes_typescript_tier2_heritage_edges() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let implementors = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Animal".into(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        implementors.iter().any(|h| {
            h.type_qualified == "Dog"
                && h.interface_qualified.as_deref() == Some("Animal")
                && h.kind == "inherit"
                && h.path == "src/pets.ts"
        }),
        "TypeScript inheritance edge missing by interface query: {implementors:?}"
    );
}

#[test]
fn find_supertypes_includes_typescript_tier2_heritage_edges() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let bases = find_supertypes(
        &conn,
        &AnchorName::head(),
        &FindSupertypesArgs {
            name: "Dog".into(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        bases.iter().any(|h| {
            h.type_qualified == "Dog"
                && h.interface_qualified.as_deref() == Some("Animal")
                && h.kind == "inherit"
                && h.path == "src/pets.ts"
        }),
        "TypeScript inheritance edge missing by type query: {bases:?}"
    );
}

#[test]
fn find_subtypes_rejects_empty_name() {
    let (_repo, _sha) = init_repo(&[("src/lib.rs", "pub fn unused() {}\n")]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let err = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: " ".into(),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("non-empty"));
}

#[test]
fn find_supertypes_rejects_empty_name() {
    let (_repo, _sha) = init_repo(&[("src/lib.rs", "pub fn unused() {}\n")]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let err = find_supertypes(
        &conn,
        &AnchorName::head(),
        &FindSupertypesArgs {
            name: String::new(),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("non-empty"));
}
