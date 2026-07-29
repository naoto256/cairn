use cairn_core::query::{FindSymbolsArgs, find_symbols};

#[test]
fn prechange_find_symbols_args_remain_source_compatible() {
    let args = FindSymbolsArgs {
        query: Some("Widget".to_string()),
        fuzzy: false,
        kind: Some("class".to_string()),
        container: None,
        path_prefix: Some("src/".to_string()),
        limit: Some(10),
    };

    let _public_query = find_symbols;
    assert_eq!(args.query.as_deref(), Some("Widget"));
}
