use crate::query::SearchQueryInput;

pub struct SearchQueryInputIter<'a> {
    stack: Vec<&'a mut SearchQueryInput>,
}

impl<'a> Iterator for SearchQueryInputIter<'a> {
    type Item = &'a mut SearchQueryInput;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let node = self.stack.pop()?;

            match node {
                SearchQueryInput::Boolean {
                    must,
                    should,
                    must_not,
                } => {
                    self.stack.extend(must_not.iter_mut().rev());
                    self.stack.extend(should.iter_mut().rev());
                    self.stack.extend(must.iter_mut().rev());
                    continue;
                }
                SearchQueryInput::Boost { query, .. } => {
                    self.stack.push(query);
                    continue;
                }
                SearchQueryInput::ConstScore { query, .. } => {
                    self.stack.push(query);
                    continue;
                }
                SearchQueryInput::ScoreFilter { query, .. } => {
                    self.stack.push(
                        query
                            .as_mut()
                            .expect("ScoreFilter's query should have been set"),
                    );
                    continue;
                }
                SearchQueryInput::DisjunctionMax { disjuncts, .. } => {
                    self.stack.extend(disjuncts.iter_mut().rev());
                    continue;
                }
                SearchQueryInput::WithIndex { query, .. } => {
                    self.stack.push(query);
                    continue;
                }

                _ => {}
            }

            return Some(node);
        }
    }
}

impl SearchQueryInput {
    pub fn iter_mut(&mut self) -> SearchQueryInputIter {
        SearchQueryInputIter { stack: vec![self] }
    }
}

impl<'a> IntoIterator for &'a mut SearchQueryInput {
    type Item = &'a mut SearchQueryInput;
    type IntoIter = SearchQueryInputIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
