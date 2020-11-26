use chrono::Duration;

use crate::attrib::*;
use crate::objects;
use crate::iterators::ItemWithData;

macro_rules! impl_attribute_definition {
    [$object:ty, $attribute:ident] => {
        #[derive(Eq, PartialEq, Copy, Clone, Hash)] pub struct $attribute;
        impl Attribute for $attribute { type Object = $object; }
    }
}

// macro_rules! impl_attribute_select {
//     [! $object:ty, $attribute:ident, $small_type:ty] => {
//         impl Select for $attribute {
//             type Item = $object;
//             type IntoItem = $small_type;
//             fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
//                 Self::get(item_with_data)
//             }
//         }
//     };
//     [? $object:ty, $attribute:ident, $small_type:ty] => {
//         impl Select for $attribute {
//             type Item = $object;
//             type IntoItem = Option<$small_type>;
//             fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
//                 Self::get(item_with_data)
//             }
//         }
//     }
// }

macro_rules! impl_attribute_getter {
    [! $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl Getter for $attribute {
            type IntoItem = $small_type;
            fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl OptionGetter for $attribute {
            type IntoItem = $small_type;
            fn get_opt(object: &ItemWithData<Self::Object>) -> Option<Self::IntoItem> {
                Some(object.$getter())
            }
        }
    };
    [? $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl Getter for $attribute {
            type IntoItem = Option<$small_type>;
            fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem {
                object.$getter()
            }
        }
        impl OptionGetter for $attribute {
            type IntoItem = $small_type;
            fn get_opt(object: &ItemWithData<Self::Object>) -> Option<Self::IntoItem> {
                object.$getter()
            }
        }
    }
}

// macro_rules! impl_attribute_sort {
//     [! $object:ty, $attribute:ident] => {
//         impl Sort for $attribute {
//             type Item = $object;
//             fn sort_ascending(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
//                 vector.sort_by_key(|item_with_data| Self::get(item_with_data).unwrap())
//             }
//         }
//     };
//     [? $object:ty, $attribute:ident] => {
//         impl Sort for $attribute {
//             type Item = $object;
//             fn sort_ascending(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
//                 vector.sort_by_key(|e| Self::get(e))
//             }
//         }
//     };
// }

// macro_rules! impl_attribute_group {
//     [! $object:ty, $attribute:ident, $small_type:ty] => {
//         impl Group for $attribute {
//             type Key = Option<$small_type>;
//             type Item = $object;
//             fn select_key(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::Key {
//                 Self::get(item_with_data)
//             }
//         }
//     };
//     [? $object:ty, $attribute:ident, $small_type:ty] => {
//         impl Group for $attribute {
//             type Key = $small_type;
//             type Item = $object;
//             fn select_key(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::Key {
//                 Self::get(item_with_data).unwrap()
//             }
//         }
//     }
// }

macro_rules! impl_attribute_count {
    [! $object:ty, $attribute:ident, $counter:ident] => {
        impl Countable for $attribute {
            fn count(object: &ItemWithData<Self::Object>) -> Option<usize> {
                Some(object.$counter())
            }
        }
    };
    [? $object:ty, $attribute:ident, $counter:ident] => {
        impl Countable for $attribute {
            fn count(object: &ItemWithData<Self::Object>) -> Option<usize> {
                object.$counter()
            }
        }
    }
}

macro_rules! impl_attribute_filter {
    [$object:ty, $attribute:ident] => {
        impl Filter for $attribute {
            type Item = $object;
            fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
                Self::get(item_with_data).unwrap_or(false)
            }
        }
    }
    // ($object:ty, $attribute:ident, $small_type:ty) => {
    //     impl Filter for $attribute {
    //         type Item = $object;
    //         fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
    //             Self::get(item_with_data).is_some()
    //         }
    //     }
    // }
}

macro_rules! impl_attribute {
    [! $object:ty, $attribute:ident, bool, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![! $object, $attribute, bool, $getter];
        //impl_attribute_select![! $object, $attribute, bool];
        //impl_attribute_sort![! $object, $attribute];
        //impl_attribute_group![! $object, $attribute, bool];
        impl_attribute_filter![$object, $attribute];
    };
    [! $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![! $object, $attribute, $small_type, $getter];
        //impl_attribute_select![! $object, $attribute, $small_type];
        //impl_attribute_sort![! $object, $attribute];
        //impl_attribute_group![! $object, $attribute, $small_type];
    };
    [? $object:ty, $attribute:ident, bool, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![? $object, $attribute, bool, $getter];
        //impl_attribute_select![? $object, $attribute, bool];
        //impl_attribute_sort![? $object, $attribute];
        //impl_attribute_group![? $object, $attribute, bool];
        impl_attribute_filter![$object, $attribute];
    };
    [? $object:ty, $attribute:ident, $small_type:ty, $getter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![? $object, $attribute, $small_type, $getter];
        //impl_attribute_select![? $object, $attribute, $small_type];
        //impl_attribute_sort![? $object, $attribute];
        //impl_attribute_group![? $object, $attribute, $small_type];
    };
    [!.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![! $object, $attribute, Vec<$small_type>, $getter];
        //impl_attribute_select![! $object, $attribute, Vec<$small_type>];
        impl_attribute_count![! $object, $attribute, $counter];
    };
    [?.. $object:ty, $attribute:ident, $small_type:ty, $getter:ident, $counter:ident] => {
        impl_attribute_definition![$object, $attribute];
        impl_attribute_getter![? $object, $attribute, Vec<$small_type>, $getter];
        //impl_attribute_select![? $object, $attribute, Vec<$small_type>];
        impl_attribute_count![? $object, $attribute, $counter];
    };
}

pub mod project {
    use crate::query::*;
    impl_attribute![!   objects::Project, Id, objects::ProjectId, id];
    impl_attribute![!   objects::Project, URL, String, url];
    impl_attribute![?   objects::Project, Issues, usize, issue_count];
    impl_attribute![?   objects::Project, BuggyIssues, usize, buggy_issue_count];
    impl_attribute![?   objects::Project, IsFork, bool, is_fork];
    impl_attribute![?   objects::Project, IsArchived, bool, is_archived];
    impl_attribute![?   objects::Project, IsDisabled, bool, is_disabled];
    impl_attribute![?   objects::Project, Stars, usize, star_count];
    impl_attribute![?   objects::Project, Watchers, usize, watcher_count];
    impl_attribute![?   objects::Project, Size, usize, size];
    impl_attribute![?   objects::Project, OpenIssues, usize, open_issue_count];
    impl_attribute![?   objects::Project, Forks, usize, fork_count];
    impl_attribute![?   objects::Project, Subscribers, usize, subscriber_count];
    impl_attribute![?   objects::Project, License, String, license];
    impl_attribute![?   objects::Project, Language, objects::Language, language];
    impl_attribute![?   objects::Project, Description, String, description];
    impl_attribute![?   objects::Project, Homepage, String, homepage];
    impl_attribute![?   objects::Project, HasIssues, bool, has_issues];
    impl_attribute![?   objects::Project, HasDownloads, bool, has_downloads];
    impl_attribute![?   objects::Project, HasWiki, bool, has_wiki];
    impl_attribute![?   objects::Project, HasPages, bool, has_pages];
    impl_attribute![?   objects::Project, Created, i64, created];
    impl_attribute![?   objects::Project, Updated, i64, updated];
    impl_attribute![?   objects::Project, Pushed, i64, pushed];
    impl_attribute![?   objects::Project, DefaultBranch, String, default_branch];
    impl_attribute![?   objects::Project, Age, Duration, lifetime];
    impl_attribute![?.. objects::Project, Heads, objects::Head, heads, head_count];
    impl_attribute![?.. objects::Project, Commits, objects::Commit, commits, commit_count];
    impl_attribute![?.. objects::Project, Authors, objects::User, authors, author_count];
    impl_attribute![?.. objects::Project, Committers, objects::User, committers, committer_count];
    impl_attribute![?.. objects::Project, Users, objects::User, users, user_count];
    impl_attribute![?.. objects::Project, Paths, objects::Path, paths, path_count];
    impl_attribute![?.. objects::Project, Snapshots, objects::Snapshot, snapshots, snapshot_count];
}

pub mod commit {
    use crate::query::*;
    impl_attribute![!   objects::Commit, Id, objects::CommitId, id];
    impl_attribute![!   objects::Commit, Committer, objects::User, committer];
    impl_attribute![!   objects::Commit, Author, objects::User, author];
    impl_attribute![?   objects::Commit, Hash, String, hash];
    impl_attribute![?   objects::Commit, Message, String, message];
    impl_attribute![?   objects::Commit, AuthoredTimestamp, i64, author_timestamp];
    impl_attribute![?   objects::Commit, CommittedTimestamp, i64, committer_timestamp];
    impl_attribute![?.. objects::Commit, Paths, objects::Path, changed_paths, changed_path_count];
    impl_attribute![?.. objects::Commit, Snapshots, objects::Snapshot, changed_snapshots, changed_snapshot_count];
    impl_attribute![!.. objects::Commit, Parents, objects::Commit, parents, parent_count];
}

pub mod head {
    use crate::query::*;
    impl_attribute![!   objects::Head, Name, String, name];
    impl_attribute![!   objects::Head, Commit, objects::Commit, commit];
}

pub mod user {
    use crate::query::*;
    impl_attribute![!   objects::User, Id, objects::UserId, id];
    impl_attribute![!   objects::User, Email, String, email];
    impl_attribute![?   objects::User, AuthorExperience, Duration, author_experience];
    impl_attribute![?   objects::User, CommitterExperience, Duration, committer_experience];
    impl_attribute![?   objects::User, Experience, Duration, experience];
    impl_attribute![?.. objects::User, AuthoredCommits, objects::Commit, authored_commits, authored_commit_count];
    impl_attribute![?.. objects::User, CommittedCommits, objects::Commit, committed_commits, committed_commit_count];
}

pub mod path {
    use crate::query::*;
    impl_attribute![!   objects::Path, Id, objects::PathId, id];
    impl_attribute![!   objects::Path, Location, String, location];
    impl_attribute![?   objects::Path, Language, objects::Language, language];
}

pub mod snapshot {
    use crate::query::*;
    impl_attribute![!   objects::Snapshot, Id, objects::SnapshotId, id];
    impl_attribute![!   objects::Snapshot, Bytes, Vec<u8>, raw_contents_owned];
    impl_attribute![!   objects::Snapshot, Contents, String, contents_owned];
}

pub mod require {
    use crate::query::*;
    use crate::attrib::*;
    use crate::iterators::ItemWithData;

    macro_rules! impl_comparison {
        ($name:ident, $trait_limit:ident, $comparator:ident, $default:expr) => {
            pub struct $name<A, N>(pub A, pub N) where A: OptionGetter<IntoItem=N>;
            impl<A, N, T> Filter for $name<A, N> where A: OptionGetter<IntoItem=N> + Attribute<Object=T>, N: $trait_limit {
                type Item = T;
                fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
                    A::get_opt(item_with_data).map_or(true, |n| n.$comparator(&self.1))
                }
            }
        }
    }
                                               /* vs None */
    impl_comparison!(LessThan, PartialOrd, lt, false);
    impl_comparison!(AtMost,   PartialOrd, le, false);
    impl_comparison!(Exactly,  Eq,         eq, false);
    impl_comparison!(AtLeast,  PartialOrd, ge, true);
    impl_comparison!(MoreThan, PartialOrd, gt, true);

    macro_rules! impl_binary {
        ($name:ident, $comparator:expr) => {
            pub struct $name<A, B>(pub A, pub B) where A: Filter, B: Filter;
            impl<A, B, T> Filter for $name<A, B> where A: Filter<Item=T>, B: Filter<Item=T> {
                type Item = T;
                fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
                    $comparator(self.0.accept(item_with_data),
                                self.1.accept(item_with_data))
                }
            }
        }
    }

    impl_binary!(And, |a, b| a && b); // TODO Effectively does not short circuit.
    impl_binary!(Or,  |a, b| a || b);

    macro_rules! impl_unary {
        ($name:ident, $comparator:expr) => {
            pub struct $name<A>(pub A) where A: Filter;
            impl<A, T> Filter for $name<A> where A: Filter<Item=T> {
                type Item = T;
                fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
                    $comparator(self.0.accept(item_with_data))
                }
            }
        }
    }

    impl_unary!(Not,  |a: bool| !a);

    macro_rules! impl_existential {
        ($name:ident, $method:ident) => {
            pub struct $name<A>(pub A) where A: OptionGetter;
            impl<A, T> Filter for $name<A> where A: OptionGetter, A: Attribute<Object=T> {
                type Item = T;
                fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
                    A::get_opt(item_with_data).$method()
                }
            }
        }
    }

    impl_existential!(Exists,  is_some);
    impl_existential!(Missing, is_none);

    pub struct Same<'a, A>(pub A, pub &'a str) where A: OptionGetter;
    impl<'a, A, T> Filter for Same<'a, A> where A: OptionGetter<IntoItem=String>, A: Attribute<Object=T> {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
            A::get_opt(item_with_data).map_or(false, |e| e.as_str() == self.1)
        }
    }

    pub struct Contains<'a, A>(pub A, pub &'a str) where A: OptionGetter;
    impl<'a, A, T> Filter for Contains<'a, A> where A: OptionGetter<IntoItem=String>, A: Attribute<Object=T> {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
            A::get_opt(item_with_data).map_or(false, |e| e.contains(self.1))
        }
    }

    #[macro_export] macro_rules! regex { ($str:expr) => { regex::Regex::new($str).unwrap() }}
    pub struct Matches<A>(pub A, pub regex::Regex) where A: OptionGetter;
    impl<A, T> Filter for  Matches<A> where A: OptionGetter<IntoItem=String>, A: Attribute<Object=T> {
        type Item = T;
        fn accept(&self, item_with_data: &ItemWithData<Self::Item>) -> bool {
            A::get_opt(item_with_data).map_or(false, |e| self.1.is_match(&e))
        }
    }
}

pub mod stats {
    use std::hash::Hash;
    use itertools::Itertools;

    use crate::query::*;
    use crate::attrib::*;
    use crate::iterators::ItemWithData;
    use crate::piracy::OptionPiracy;
    use crate::ordf64::OrdF64;
    use std::iter::Sum;

    pub struct Count<A: Countable>(pub A);
    impl<A, T> Attribute for Count<A> where A: Attribute<Object=T> + Countable {
        type Object = T;
    }
    impl<A, T> Getter for Count<A> where A: Attribute<Object=T> + Countable {
        type IntoItem = Option<usize>;
        fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem {
            A::count(object)
        }
    }
    impl<A, T> OptionGetter for Count<A> where A: Attribute<Object=T> + Countable {
        type IntoItem = usize;
        fn get_opt(object: &ItemWithData<Self::Object>) -> Option<Self::IntoItem> {
            A::count(object)
        }
    }
    // impl<A, T> Sort for Count<A> where A: Attribute<Object=T> + Countable  {
    //     type Item = T;
    //     fn sort_ascending(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
    //         vector.sort_by_key(|item_with_data| Self::get(item_with_data))
    //     }
    // }
    // impl<A, T> Select for Count<A> where A: Attribute<Object=T> + Countable  {
    //     type Item = T;
    //     type IntoItem = Option<usize>;
    //     fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
    //         Self::get(item_with_data)
    //     }
    // }
    // impl<A, T> Group for Count<A> where A: Attribute<Object=T> + Countable {
    //     type Key = Option<usize>;
    //     type Item = T;
    //     fn select_key(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::Key {
    //         Self::get(item_with_data)
    //     }
    // }

    // TODO bucket
    pub struct Bin;
    pub struct Bucket;

    macro_rules! impl_minmax {
        ($name:ident, $selector:ident -> $result:ty) => {
            pub struct $name<A: Attribute>(pub A);
            impl<A, T> Attribute for $name<A> where A: Attribute<Object=T> {
                type Object = T;
            }
            // impl<A, I, T> Getter for $name<A> where A: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>>, I: Ord + Clone {
            //     type IntoItem = Option<$return>;
            //     fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem {
            //         A::get_opt(object).map($selector).flatten()
            //     }
            // }
            impl<A, I, T> Getter for $name<A> where A: Attribute<Object=T> + Getter<IntoItem=Vec<I>>, I: Ord + Clone {
                type IntoItem = Option<$result>;
                fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem {
                    A::get(object).into_iter().$selector()
                }
            }
            impl<A, I, T> OptionGetter for $name<A> where A: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>>, I: Ord + Clone {
                type IntoItem = $result;
                fn get_opt(object: &ItemWithData<Self::Object>) -> Option<Self::IntoItem> {
                    A::get_opt(object).map(|e| e.into_iter().$selector()).flatten()
                }
            }
            // impl<A, I, T> Sort for $name<A> where A: Attribute<Object=T> + Getter<IntoItem=Vec<I>>, I: Ord + Clone {
            //     type Item = T;
            //     fn sort_ascending(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
            //         vector.sort_by_key(|item_with_data| Self::get(item_with_data))
            //     }
            // }
            // impl<A, I, T> Select for $name<A> where A: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>>, I: Ord + Clone {
            //     type Item = T;
            //     type IntoItem = Option<$return>;
            //     fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
            //         Self::get_opt(item_with_data)
            //     }
            // }
            // impl<A, I, T> Group for $name<A> where A: Attribute<Object=T> + Getter<IntoItem=Vec<I>>, I: Ord + Clone + Hash {
            //     type Key = Option<$return>;
            //     type Item = T;
            //     fn select_key(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::Key {
            //         Self::get(item_with_data)
            //     }
            // }
        }
    }

    trait OptionMinMax<T: PartialOrd + Clone>: Iterator<Item=T> + Sized {
        fn minmax_into_option(self) -> Option<(T, T)> { Itertools::minmax(self).into_option() }
    }
    impl<I, T> OptionMinMax<T> for I where I: Iterator<Item=T> + Sized, T: PartialOrd + Clone {}

    //TODO min_by/max_by/minmax_by
    impl_minmax!(Min,    min -> I);
    impl_minmax!(Max,    max -> I);
    impl_minmax!(MinMax, minmax_into_option -> (I, I));

    pub struct Mean<A: Attribute>(pub A);
    impl<A, T> Attribute for Mean<A> where A: Attribute<Object=T> {
        type Object = T;
    }
    impl<A> Mean<A> where A: Attribute {
        fn calculate_mean<N>(vector: Vec<N>) -> OrdF64 where N: Sum + Into<f64> {
            let length = vector.len() as f64;
            let sum = vector.into_iter().sum::<N>().into();
            OrdF64::from(sum / length)
        }
    }
    impl<A, I, T> Getter for Mean<A>
        where I: Sum + Into<f64>, // Just Into<f64>?
              A: Attribute<Object=T> + Getter<IntoItem=Vec<I>> + Sum<I> {

        type IntoItem = OrdF64;
        fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem {
            Self::calculate_mean(A::get(object))
        }
    }
    impl<A, I, T> OptionGetter for Mean<A>
        where I: Sum + Into<f64>, // Just Into<f64>?
              A: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>> + Sum<I> {

        type IntoItem = OrdF64;
        fn get_opt(object: &ItemWithData<Self::Object>) -> Option<Self::IntoItem> {
            A::get_opt(object).map(|vector| Self::calculate_mean(vector))
        }
    }
    // impl<A, I, T> Sort for Mean<A>
    //     where I: Sum + Into<f64>,
    //           A: Attribute<Object=T> + Countable + Getter<IntoItem=Vec<I>> + Sum<I> {
    //
    //     type Item = T;
    //     fn sort_ascending(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
    //         vector.sort_by_key(|item_with_data| Self::get(item_with_data))
    //     }
    // }
    // impl<A, I, T> Select for Mean<A>
    //     where I: Sum + Into<f64>,
    //           A: Attribute<Object=T> + Countable + OptionGetter<IntoItem=Vec<I>> + Sum<I> {
    //
    //     //type Item = T;
    //     type IntoItem = Option<OrdF64>;
    //     fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
    //         Self::get_opt(item_with_data)
    //     }
    // }
    // impl<A, I, T> Group for Mean<A>
    //     where I: Sum + Into<f64>,
    //           A: Attribute<Object=T> + Countable + Getter<IntoItem=Vec<I>> + Sum<I> {
    //
    //     type Key = Option<OrdF64>;
    //     type Item = T;
    //     fn select_key(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::Key {
    //         Self::get(item_with_data)
    //     }
    // }

    pub struct Median<A: Attribute>(pub A);
    impl<A, T> Attribute for Median<A> where A: Attribute<Object=T> {
        type Object = T;
    }
    impl<A> Median<A> where A: Attribute {
        fn calculate_median<N>(mut items: Vec<N>) -> Option<OrdF64>
            where N: Sum + Ord + Into<f64> + Clone {
            //let items: Vec<N> = vector.into_iter().sorted().collect();
            items.sort();
            let length = items.len();
            if length == 0 {
                None
            } else {
                let value: f64 =
                    if length == 1 {
                        items[0].clone().into()
                    } else if length % 2 != 0usize {
                        items[length / 2].clone().into()
                    } else {
                        let left = items[(length / 2) - 1].clone().into();
                        let right = items[(length / 2)].clone().into();
                        (left + right) / 2f64
                    };
                Some(OrdF64::from(value))
            }
        }
    }
    impl<A, I, T> Getter for Median<A>
        where I: Sum + Ord + Into<f64> + Clone, // Just Into<f64>?
              A: Attribute<Object=T> + Getter<IntoItem=Vec<I>> + Sum<I> {
        type IntoItem = Option<OrdF64>; // TODO None == NAN
        fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem {
            Self::calculate_median(A::get(object))
        }
    }
    impl<A, I, T> OptionGetter for Median<A>
        where I: Sum + Ord + Into<f64> + Clone, // Just Into<f64>?
              A: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>> + Sum<I> {
        type IntoItem = OrdF64;
        fn get_opt(object: &ItemWithData<Self::Object>) -> Option<Self::IntoItem> {
            A::get_opt(object).map(|v| {
                Self::calculate_median(v)
            }).flatten()
        }
    }
    // impl<A, I, T> Sort for Median<A>
    //     where I: Sum + Ord + Into<f64> + Clone,
    //           A: Attribute<Object=T> + Countable + Getter<IntoItem=Vec<I>> + Sum<I> {
    //
    //     type Item = T;
    //     fn sort_ascending(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
    //         vector.sort_by_key(|item_with_data| Self::get(item_with_data))
    //     }
    // }
    // impl<A, I, T> Select for Median<A>
    //     where I: Sum + Ord + Into<f64> + Clone,
    //           A: Attribute<Object=T> + Countable + OptionGetter<IntoItem=Vec<I>> + Sum<I> {
    //
    //     type Item = T;
    //     type IntoItem = Option<OrdF64>;
    //     fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
    //         Self::get_opt(item_with_data)
    //     }
    // }
    // impl<A, I, T> Group for Median<A>
    //     where I: Sum + Ord + Into<f64> + Clone,
    //           A: Attribute<Object=T> + Countable + Getter<IntoItem=Vec<I>> + Sum<I> {
    //
    //     type Key = Option<OrdF64>;
    //     type Item = T;
    //     fn select_key(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::Key {
    //         Self::get(item_with_data)
    //     }
    // }
}

pub mod retrieve {
    use crate::attrib::*;
    use crate::iterators::ItemWithData;

    //From(project::Commits, commit::Author)

    pub struct From<O: Attribute, A: Attribute> (pub O, pub A);
    impl<O, A, T, I> Attribute for From<O, A>
        where O: Attribute<Object=T> + OptionGetter<IntoItem=I>, A: Attribute<Object=I> {
        type Object = T;
    }
    impl<O, A, T, I, E> Getter for From<O, A>
         where O: Attribute<Object=T> + OptionGetter<IntoItem=I>,
               A: Attribute<Object=I> + Getter<IntoItem=E> {
         type IntoItem = Option<E>;
         fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem {
            O::get_opt_with_data(object).map(|object| A::get(&object))
         }
    }
    impl<O, A, T, I, E> OptionGetter for From<O, A>
        where O: Attribute<Object=T> + OptionGetter<IntoItem=I>,
              A: Attribute<Object=I> + OptionGetter<IntoItem=E> {
        type IntoItem = E;
        fn get_opt(object: &ItemWithData<Self::Object>) -> Option<Self::IntoItem> {
            O::get_opt_with_data(object).map(|object| A::get_opt(&object)).flatten()
        }
    }
    // impl<O, A, T, I, E> Sort for From<O, A>
    //     where O: Attribute<Object=T> + Getter<IntoItem=I>,
    //           A: Attribute<Object=I> + Getter<IntoItem=E>,
    //           E: Ord {
    //
    //     type Item = T;
    //     fn sort_ascending(&self, vector: &mut Vec<ItemWithData<Self::Item>>) {
    //         vector.sort_by_key(|item_with_data| Self::get(item_with_data))
    //     }
    // }

    pub struct FromEach<O: Attribute, A: Attribute> (pub O, pub A);
    impl<O, A, T, I> Attribute for FromEach<O, A>
        where O: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>>, A: Attribute<Object=I> {
        type Object = T;
    }
    impl<O, A, T, I, E> Getter for FromEach<O, A>
        where O: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>>,
              A: Attribute<Object=I> + Getter<IntoItem=E> {
        type IntoItem = Option<Vec<E>>;
        fn get(object: &ItemWithData<Self::Object>) -> Self::IntoItem {
            O::get_opt_each_with_data(object).map(|v| {
                v.iter().map(|object| { A::get(object) }).collect()
            })
        }
    }
    impl<O, A, T, I, E> OptionGetter for FromEach<O, A>
        where O: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>>,
              A: Attribute<Object=I> + OptionGetter<IntoItem=E> {
        type IntoItem = Vec<E>;
        fn get_opt(object: &ItemWithData<Self::Object>) -> Option<Self::IntoItem> {
            O::get_opt_each_with_data(object).map(|v| {
                v.iter().flat_map(|object| { A::get_opt(object) }).collect()
            })
        }
    }
    // impl<O, A, T, I, E> Select for FromEach<O,A>
    //     where O: Attribute<Object=T> + OptionGetter<IntoItem=Vec<I>>,
    //           A: Attribute<Object=I> + OptionGetter<IntoItem=E> {
    //
    //     type Item = T;
    //     type IntoItem = Vec<E>;
    //     fn select(&self, item_with_data: &ItemWithData<Self::Item>) -> Self::IntoItem {
    //         Self::get_opt(item_with_data).unwrap_or(Vec::new())
    //     }
    // }
}