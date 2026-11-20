use bauplan_api::gropius::client::ClientError;
use bauplan_api::pagination::{ListQuery, Page};

/// Iterates the items of a list endpoint page by page, up to `limit` items.
///
/// Every page asks for the items still missing, or for none in particular
/// without a limit, and the server decides how many it returns. The
/// filters go with every page. We ask for the first page right away, so an
/// error on it comes back before the caller prints anything.
pub(crate) fn paginate<T, F, E>(
    limit: Option<usize>,
    filters: F,
    mut fetch: impl FnMut(ListQuery<F>) -> Result<Page<T>, ClientError<E>>,
) -> anyhow::Result<impl Iterator<Item = anyhow::Result<T>>>
where
    F: Clone,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut remaining = limit;
    let mut pagination_token = None;
    let mut items = Vec::new().into_iter();
    let mut last_page = false;

    let mut all_items = std::iter::from_fn(move || {
        loop {
            if remaining == Some(0) {
                return None;
            }

            if let Some(item) = items.next() {
                if let Some(remaining) = remaining.as_mut() {
                    *remaining -= 1;
                }
                return Some(Ok(item));
            }

            if last_page {
                return None;
            }

            let query = ListQuery {
                pagination_token: pagination_token.take(),
                limit: remaining.map(|n| u32::try_from(n).unwrap_or(u32::MAX)),
                filters: filters.clone(),
            };
            let page = match fetch(query) {
                Ok(page) => page,
                Err(err) => {
                    last_page = true;
                    return Some(Err(err.into()));
                }
            };

            items = page.items.into_iter();
            last_page = page.pagination_token.is_none();
            pagination_token = page.pagination_token;
        }
    })
    .peekable();

    if let Some(Err(err)) = all_items.next_if(Result::is_err) {
        return Err(err);
    }

    Ok(all_items)
}
