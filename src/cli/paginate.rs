use bauplan_api::pagination::{ListQuery, Page};

/// Iterates the items of a list endpoint page by page, up to `limit` items,
/// starting from `pagination_token` when there is one.
///
/// Each page asks for the items still missing. Without a limit, it asks for
/// no number and the server picks one. The filters go with every page. We ask for the first page right away, so an
/// error on it comes back before the caller prints anything.
pub(crate) fn paginate<T, F, Fetch>(
    limit: Option<u32>,
    pagination_token: Option<String>,
    filters: F,
    fetch: Fetch,
) -> anyhow::Result<Pages<T, F, Fetch>>
where
    F: Clone,
    Fetch: FnMut(ListQuery<F>) -> anyhow::Result<Page<T>>,
{
    let mut pages = Pages {
        remaining: limit,
        pagination_token,
        items: Vec::new().into_iter(),
        last_page: false,
        filters,
        fetch,
    };

    pages.next_page()?;
    Ok(pages)
}

pub(crate) struct Pages<T, F, Fetch> {
    remaining: Option<u32>,
    pagination_token: Option<String>,
    items: std::vec::IntoIter<T>,
    last_page: bool,
    filters: F,
    fetch: Fetch,
}

impl<T, F, Fetch> Pages<T, F, Fetch>
where
    F: Clone,
    Fetch: FnMut(ListQuery<F>) -> anyhow::Result<Page<T>>,
{
    /// Reads the rest of the list into one page. If we stop at the limit before
    /// the end, the page also has the token to continue.
    pub(crate) fn into_page(mut self) -> anyhow::Result<Page<T>> {
        let items = self.by_ref().collect::<anyhow::Result<_>>()?;
        Ok(Page {
            items,
            pagination_token: self.pagination_token,
        })
    }

    fn next_page(&mut self) -> anyhow::Result<()> {
        let query = ListQuery {
            pagination_token: self.pagination_token.take(),
            limit: self.remaining,
            filters: self.filters.clone(),
        };
        let page = (self.fetch)(query)?;

        self.items = page.items.into_iter();
        self.last_page = page.pagination_token.is_none();
        self.pagination_token = page.pagination_token;
        Ok(())
    }
}

impl<T, F, Fetch> Iterator for Pages<T, F, Fetch>
where
    F: Clone,
    Fetch: FnMut(ListQuery<F>) -> anyhow::Result<Page<T>>,
{
    type Item = anyhow::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.remaining == Some(0) {
                return None;
            }

            if let Some(item) = self.items.next() {
                if let Some(remaining) = self.remaining.as_mut() {
                    *remaining -= 1;
                }
                return Some(Ok(item));
            }

            if self.last_page {
                return None;
            }

            if let Err(err) = self.next_page() {
                self.last_page = true;
                return Some(Err(err));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stops_at_the_limit_with_the_next_token() -> anyhow::Result<()> {
        // limit, start token, items, page limits asked, token
        let cases = [
            (None, None, 0..10, vec![None, None, None], None),
            (Some(3), None, 0..3, vec![Some(3)], Some("3")),
            (Some(6), None, 0..6, vec![Some(6), Some(2)], Some("6")),
            (
                Some(10),
                None,
                0..10,
                vec![Some(10), Some(6), Some(2)],
                None,
            ),
            (Some(3), Some("8"), 8..10, vec![Some(3)], None),
        ];

        for (limit, start, want_items, want_asked, want_token) in cases {
            // A server with the items 0..10 that returns at most 4 per page.
            let mut asked = Vec::new();
            let pages = paginate(limit, start.map(str::to_owned), (), |query| {
                asked.push(query.limit);
                let from = query
                    .pagination_token
                    .map_or(Ok(0), |token| token.parse())?;
                let to = (from + query.limit.unwrap_or(4).min(4)).min(10);
                anyhow::Ok(Page {
                    items: (from..to).collect(),
                    pagination_token: (to < 10).then(|| to.to_string()),
                })
            })?;

            let page = pages.into_page()?;
            assert_eq!(
                page.items,
                want_items.collect::<Vec<_>>(),
                "{limit:?} {start:?}"
            );
            assert_eq!(asked, want_asked, "{limit:?} {start:?}");
            assert_eq!(
                page.pagination_token.as_deref(),
                want_token,
                "{limit:?} {start:?}"
            );
        }

        Ok(())
    }
}
