use crate::anchor::AnchorName;
use crate::cas::store;
use crate::query::{FindImplsArgs, find_impls};
use crate::register::register_repo;
use crate::testutil::init_repo;

#[test]
fn find_impls_includes_typescript_tier2_heritage_edges() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let implementors = find_impls(
        &conn,
        &AnchorName::head(),
        &FindImplsArgs {
            interface_qualified: Some("Animal".into()),
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

    let bases = find_impls(
        &conn,
        &AnchorName::head(),
        &FindImplsArgs {
            type_qualified: Some("Dog".into()),
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
