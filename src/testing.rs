use std::collections::*;
use std::iter::*;

use chrono::DateTime;


use crate::Djanco;
use crate::data::*;
use crate::objects::*;
use crate::store;
use crate::stores;
use crate::Store;
use parasite::StoreKind::Generic;

fn database() -> Database {
    //let store = DatastoreView::new("/dejacode/testing/10b", now.into());

    let database = Djanco::from_store("/data/djcode/example-dataset/", timestamp!(March 2021), store![JavaScript]).unwrap();

    database
}

macro_rules! show_attribute {
    ($database:expr, $attribute:expr) => {{
        let max_len =
            $attribute.iter().map(|(_, e)| e.to_string().len()).max().unwrap_or(0);
        for (id, value) in $attribute.iter() {
            println!("({}, {:max_len$}  // {}",
                     id, format!("{})", value),
                     $database.project(id).map_or(String::new(), |p| p.url()),
                     max_len = max_len + 1);
        }
    }};
    ($database:expr, optional $attribute:expr) => {{
        let max_len = $attribute.iter()
            .map(|(_, e)| e.as_ref().map_or(0, |e| format!("{}", e).len())).max().unwrap_or(0);
        for (id, value) in $attribute.iter() {
            println!("({}, {:max_len$}  // {}",
                     id, format!("{})", value.as_ref().unwrap()),
                     $database.project(id).map_or(String::new(), |p| p.url()),
                     max_len = max_len + 1);
        }
    }}
}

macro_rules! check_project_attrib_value {
        ($method:ident -> $type:ty, $ex_converter:expr, $ac_converter:expr, $($values:expr),+) => {{
            let database = database();

            let expected: BTreeMap<ProjectId, $type> = BTreeMap::from_iter(vec![
                $($values,)+
            ].into_iter().map(|(i, e) | (ProjectId::from(i as usize), $ex_converter(e))));

            let actual: BTreeMap<ProjectId, $type> =
                BTreeMap::from_iter(database.projects().map(|p| (p.id(), $ac_converter(p.$method()))));

            println!("expected:");
            show_attribute!(database, expected);
            println!();
            println!("actual:");
            show_attribute!(database, actual);

            assert_eq!(expected, actual);
        }};
        ($method:ident -> optional $type:ty, $ex_converter:expr, $ac_converter:expr, $($values:expr),+) => {{
            let database = database();

            let expected: BTreeMap<ProjectId, Option<$type>> = BTreeMap::from_iter(vec![
                $($values,)+
            ].into_iter().map(|(i, e) | (ProjectId::from(i as usize), $ex_converter(e))));

            let actual: BTreeMap<ProjectId, Option<$type>> =
                BTreeMap::from_iter(database.projects().map(|p| (p.id(), $ac_converter(p.$method()))));

            println!("expected:");
            show_attribute!(database, optional expected);
            println!();
            println!("actual:");
            show_attribute!(database, optional actual);

            assert_eq!(expected, actual);
        }}
    }

#[test] fn project_urls () {
    check_project_attrib_value!(url -> String, |e: &str| e.to_owned(), |e| e,
            (0, "https://github.com/nodejs/node.git"),
            (1, "https://github.com/pixijs/pixi.js.git"),
            (2, "https://github.com/angular/angular.git"),
            (3, "https://github.com/apache/airflow.git"),
            (4, "https://github.com/facebook/react.git"),
            (5, "https://github.com/vuejs/vue.git"),
            (6, "https://github.com/xonsh/xonsh.git"),
            (7, "https://github.com/meteor/meteor.git"),
            (8, "https://github.com/3b1b/manim.git"),
            (9, "https://github.com/s0md3v/photon.git")
        );
}

#[test] fn project_languages() {
    check_project_attrib_value!(language -> optional Language, |e: Language| Some(e), |e| e,
            (0, Language::JavaScript), //https://github.com/nodejs/node.git
            (1, Language::TypeScript), //https://github.com/pixijs/pixi.js.git
            (2, Language::TypeScript), //https://github.com/angular/angular.git
            (3, Language::Python),     //https://github.com/apache/airflow.git
            (4, Language::JavaScript), //https://github.com/facebook/react.git
            (5, Language::JavaScript), //https://github.com/vuejs/vue.git
            (6, Language::Python),     //https://github.com/xonsh/xonsh.git
            (7, Language::JavaScript), //https://github.com/meteor/meteor.git
            (8, Language::Python),     //https://github.com/3b1b/manim.git
            (9, Language::Python)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_stars() {
    check_project_attrib_value!(star_count -> optional usize, |e: usize| Some(e), |e| e,
            (0, 77513),     //https://github.com/nodejs/node.git
            (1, 32137),     //https://github.com/pixijs/pixi.js.git
            (2, 71211),     //https://github.com/angular/angular.git
            (3, 20701),     //https://github.com/apache/airflow.git
            (4, 164841),    //https://github.com/facebook/react.git
            (5, 180390),    //https://github.com/vuejs/vue.git
            (6, 4475),      //https://github.com/xonsh/xonsh.git
            (7, 42325),     //https://github.com/meteor/meteor.git
            (8, 31281),     //https://github.com/3b1b/manim.git
            (9, 7668)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_watchers() {
     check_project_attrib_value!(watcher_count -> optional usize, |e: usize| Some(e), |e| e,
        (0, 77513),   // https://github.com/nodejs/node.git
        (1, 32137),   // https://github.com/pixijs/pixi.js.git
        (2, 71211),   // https://github.com/angular/angular.git
        (3, 20701),   // https://github.com/apache/airflow.git
        (4, 164841),  // https://github.com/facebook/react.git
        (5, 180390),  // https://github.com/vuejs/vue.git
        (6, 4475),    // https://github.com/xonsh/xonsh.git
        (7, 42325),   // https://github.com/meteor/meteor.git
        (8, 31281),   // https://github.com/3b1b/manim.git
        (9, 7668)     // https://github.com/s0md3v/photon.git
     );
}

#[test] fn project_subscribers() {
    check_project_attrib_value!(subscriber_count -> optional usize, |e: usize| Some(e), |e| e,
        (0, 2980),  // https://github.com/nodejs/node.git
        (1, 1059),  // https://github.com/pixijs/pixi.js.git
        (2, 3196),  // https://github.com/angular/angular.git
        (3, 739),   // https://github.com/apache/airflow.git
        (4, 6732),  // https://github.com/facebook/react.git
        (5, 6346),  // https://github.com/vuejs/vue.git
        (6, 96),    // https://github.com/xonsh/xonsh.git
        (7, 1701),  // https://github.com/meteor/meteor.git
        (8, 784),   // https://github.com/3b1b/manim.git
        (9, 302)    // https://github.com/s0md3v/photon.git
    );
}

#[test] fn project_forks() {
    check_project_attrib_value!(fork_count -> optional usize, |e: usize| Some(e), |e| e,
        (0, 19548),  // https://github.com/nodejs/node.git
        (1, 4252),   // https://github.com/pixijs/pixi.js.git
        (2, 18729),  // https://github.com/angular/angular.git
        (3, 8122),   // https://github.com/apache/airflow.git
        (4, 33088),  // https://github.com/facebook/react.git
        (5, 28381),  // https://github.com/vuejs/vue.git
        (6, 475),    // https://github.com/xonsh/xonsh.git
        (7, 5155),   // https://github.com/meteor/meteor.git
        (8, 3989),   // https://github.com/3b1b/manim.git
        (9, 1083)    // https://github.com/s0md3v/photon.git
    );
}

#[test] fn project_open_issues() {
    check_project_attrib_value!(open_issue_count -> optional usize, |e: usize| Some(e), |e| e,
        (0, 1232),  // https://github.com/nodejs/node.git
        (1, 88),    // https://github.com/pixijs/pixi.js.git
        (2, 2675),  // https://github.com/angular/angular.git
        (3, 985),   // https://github.com/apache/airflow.git
        (4, 738),   // https://github.com/facebook/react.git
        (5, 564),   // https://github.com/vuejs/vue.git
        (6, 340),   // https://github.com/xonsh/xonsh.git
        (7, 132),   // https://github.com/meteor/meteor.git
        (8, 318),   // https://github.com/3b1b/manim.git
        (9, 38)     // https://github.com/s0md3v/photon.git
    );
}

#[test] fn project_sizes() {
    check_project_attrib_value!(size -> optional usize, |e: usize| Some(e), |e| e,
        (0, 665308),  // https://github.com/nodejs/node.git
        (1, 72878),   // https://github.com/pixijs/pixi.js.git
        (2, 278519),  // https://github.com/angular/angular.git
        (3, 110959),  // https://github.com/apache/airflow.git
        (4, 163004),  // https://github.com/facebook/react.git
        (5, 27929),   // https://github.com/vuejs/vue.git
        (6, 25537),   // https://github.com/xonsh/xonsh.git
        (7, 81028),   // https://github.com/meteor/meteor.git
        (8, 55336),   // https://github.com/3b1b/manim.git
        (9, 356)      // https://github.com/s0md3v/photon.git
    );
}

#[test] fn project_is_fork() {
    check_project_attrib_value!(is_fork -> optional bool, |e: bool| Some(e), |e| e,
            (0, false),     //https://github.com/nodejs/node.git
            (1, false),     //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, false),     //https://github.com/facebook/react.git
            (5, false),     //https://github.com/vuejs/vue.git
            (6, false),     //https://github.com/xonsh/xonsh.git
            (7, false),     //https://github.com/meteor/meteor.git
            (8, false),     //https://github.com/3b1b/manim.git
            (9, false)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_is_archived() {
    check_project_attrib_value!(is_archived -> optional bool, |e: bool| Some(e), |e| e,
            (0, false),     //https://github.com/nodejs/node.git
            (1, false),     //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, false),     //https://github.com/facebook/react.git
            (5, false),     //https://github.com/vuejs/vue.git
            (6, false),     //https://github.com/xonsh/xonsh.git
            (7, false),     //https://github.com/meteor/meteor.git
            (8, false),     //https://github.com/3b1b/manim.git
            (9, false)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_is_disabled() {
    check_project_attrib_value!(is_disabled -> optional bool, |e: bool| Some(e), |e| e,
            (0, false),     //https://github.com/nodejs/node.git
            (1, false),     //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, false),     //https://github.com/facebook/react.git
            (5, false),     //https://github.com/vuejs/vue.git
            (6, false),     //https://github.com/xonsh/xonsh.git
            (7, false),     //https://github.com/meteor/meteor.git
            (8, false),     //https://github.com/3b1b/manim.git
            (9, false)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_has_pages() {
    check_project_attrib_value!(has_pages -> optional bool, |e: bool| Some(e), |e| e,
            (0, false),     //https://github.com/nodejs/node.git
            (1, false),     //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, true),      //https://github.com/facebook/react.git
            (5, false),     //https://github.com/vuejs/vue.git
            (6, false),     //https://github.com/xonsh/xonsh.git
            (7, false),     //https://github.com/meteor/meteor.git
            (8, true),      //https://github.com/3b1b/manim.git
            (9, false)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_has_downloads() {
    check_project_attrib_value!(has_downloads -> optional bool, |e: bool| Some(e), |e| e,
            (0, true),     //https://github.com/nodejs/node.git
            (1, true),     //https://github.com/pixijs/pixi.js.git
            (2, true),     //https://github.com/angular/angular.git
            (3, true),     //https://github.com/apache/airflow.git
            (4, true),      //https://github.com/facebook/react.git
            (5, true),     //https://github.com/vuejs/vue.git
            (6, true),     //https://github.com/xonsh/xonsh.git
            (7, true),     //https://github.com/meteor/meteor.git
            (8, true),     //https://github.com/3b1b/manim.git
            (9, true)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_has_wiki() {
    check_project_attrib_value!(has_wiki -> optional bool, |e: bool| Some(e), |e| e,
            (0, false),     //https://github.com/nodejs/node.git
            (1, true),      //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, true),      //https://github.com/facebook/react.git
            (5, true),      //https://github.com/vuejs/vue.git
            (6, true),      //https://github.com/xonsh/xonsh.git
            (7, true),      //https://github.com/meteor/meteor.git
            (8, true),      //https://github.com/3b1b/manim.git
            (9, true)       //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_has_issues() {
    check_project_attrib_value!(has_issues -> optional bool, |e: bool| Some(e), |e| e,
            (0, true),     //https://github.com/nodejs/node.git
            (1, true),     //https://github.com/pixijs/pixi.js.git
            (2, true),     //https://github.com/angular/angular.git
            (3, true),     //https://github.com/apache/airflow.git
            (4, true),     //https://github.com/facebook/react.git
            (5, true),     //https://github.com/vuejs/vue.git
            (6, true),     //https://github.com/xonsh/xonsh.git
            (7, true),     //https://github.com/meteor/meteor.git
            (8, true),     //https://github.com/3b1b/manim.git
            (9, true)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_default_branch() {
    check_project_attrib_value!(default_branch -> optional String, |e: &str| Some(e.to_owned()), |e| e,
            (0, "master"),     //https://github.com/nodejs/node.git
            (1, "dev"),        //https://github.com/pixijs/pixi.js.git
            (2, "master"),     //https://github.com/angular/angular.git
            (3, "master"),     //https://github.com/apache/airflow.git
            (4, "master"),     //https://github.com/facebook/react.git
            (5, "dev"),        //https://github.com/vuejs/vue.git
            (6, "master"),     //https://github.com/xonsh/xonsh.git
            (7, "devel"),      //https://github.com/meteor/meteor.git
            (8, "master"),     //https://github.com/3b1b/manim.git
            (9, "master")      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_license() {
    check_project_attrib_value!(license -> optional String, |e: &str| Some(e.to_owned()), |e| e,
            (0, "Other"),                               //https://github.com/nodejs/node.git
            (1, "MIT License"),                         //https://github.com/pixijs/pixi.js.git
            (2, "MIT License"),                         //https://github.com/angular/angular.git
            (3, "Apache License 2.0"),                  //https://github.com/apache/airflow.git
            (4, "MIT License"),                         //https://github.com/facebook/react.git
            (5, "MIT License"),                         //https://github.com/vuejs/vue.git
            (6, "Other"),                               //https://github.com/xonsh/xonsh.git
            (7, "Other"),                               //https://github.com/meteor/meteor.git
            (8, "MIT License"),                         //https://github.com/3b1b/manim.git
            (9, "GNU General Public License v3.0")      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_description() {
    check_project_attrib_value!(description -> optional String, |e: &str| Some(e.to_owned()), |e| e,
            (0, "Node.js JavaScript runtime :sparkles::turtle::rocket::sparkles:"),
            (1, "The HTML5 Creation Engine: Create beautiful digital content with the fastest, most flexible 2D WebGL renderer."),
            (2, "One framework. Mobile & desktop."),
            (3, "Apache Airflow - A platform to programmatically author, schedule, and monitor workflows"),
            (4, "A declarative, efficient, and flexible JavaScript library for building user interfaces."),
            (5, "🖖 Vue.js is a progressive, incrementally-adoptable JavaScript framework for building UI on the web."),
            (6, ":shell: Python-powered, cross-platform, Unix-gazing shell"),
            (7, "Meteor, the JavaScript App Platform"),
            (8, "Animation engine for explanatory math videos"),
            (9, "Incredibly fast crawler designed for OSINT.")
        );
}
// TODO types ;-;
#[test] fn project_homepage() {
    check_project_attrib_value!(homepage -> String,
                                |e: Option<&str>| e.map_or("?".to_owned(), |e: &str| e.to_owned()),
                                |e: Option<String>| e.unwrap_or("?".to_owned()),

            (0, Some("https://nodejs.org")),
            (1, Some("http://pixijs.com")),
            (2, Some("https://angular.io")),
            (3, Some("https://airflow.apache.org/")),
            (4, Some("https://reactjs.org")),
            (5, Some("http://vuejs.org")),
            (6, Some("http://xon.sh")),
            (7, Some("https://www.meteor.com")),
            (8, None),
            (9, Some(""))
        );
}
//
#[test] fn project_created() {
    check_project_attrib_value!(created -> optional i64, |e: &str| Some(DateTime::parse_from_rfc3339(e).unwrap().timestamp()), |e| e,
            (8, "2015-03-22T18:50:58Z"),
            (6, "2015-01-21T22:05:27Z"),
            (1, "2013-01-21T22:40:50Z"),
            (9, "2018-03-30T19:38:22Z"),
            (3, "2015-04-13T18:04:58Z"),
            (4, "2013-05-24T16:15:54Z"),
            (2, "2014-09-18T16:12:01Z"),
            (7, "2012-01-19T01:58:17Z"),
            (0, "2014-11-26T19:57:11Z"),
            (5, "2013-07-29T03:24:51Z")
        );
}

#[test] fn project_updated() { // TODO harvest values
    check_project_attrib_value!(updated -> optional i64, |e: i64| Some(e), |e| e,
            (0, 1615286821),  // https://github.com/nodejs/node.git
            (1, 1615288572),  // https://github.com/pixijs/pixi.js.git
            (2, 1615288751),  // https://github.com/angular/angular.git
            (3, 1615288988),  // https://github.com/apache/airflow.git
            (4, 1615289003),  // https://github.com/facebook/react.git
            (5, 1615286492),  // https://github.com/vuejs/vue.git
            (6, 1615259076),  // https://github.com/xonsh/xonsh.git
            (7, 1615272369),  // https://github.com/meteor/meteor.git
            (8, 1615288716),  // https://github.com/3b1b/manim.git
            (9, 1615281245)   // https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_pushed() { // TODO harvest values
    check_project_attrib_value!(pushed -> optional i64, |e: i64| Some(e), |e| e,
            (0, 1615288580),  // https://github.com/nodejs/node.git
            (1, 1615230327),  // https://github.com/pixijs/pixi.js.git
            (2, 1615286446),  // https://github.com/angular/angular.git
            (3, 1615288822),  // https://github.com/apache/airflow.git
            (4, 1615282653),  // https://github.com/facebook/react.git
            (5, 1615281426),  // https://github.com/vuejs/vue.git
            (6, 1615218971),  // https://github.com/xonsh/xonsh.git
            (7, 1614948532),  // https://github.com/meteor/meteor.git
            (8, 1615211761),  // https://github.com/3b1b/manim.git
            (9, 1615141637)   // https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_head_count() {
    check_project_attrib_value!(head_count -> optional usize, |e: usize| Some(e), |e| e,
            (0, 35),    // https://github.com/nodejs/node.git
            (1, 35),    // https://github.com/pixijs/pixi.js.git
            (2, 63),    // https://github.com/angular/angular.git
            (3, 16),    // https://github.com/apache/airflow.git
            (4, 109),   // https://github.com/facebook/react.git
            (5, 59),    // https://github.com/vuejs/vue.git
            (6, 40),    // https://github.com/xonsh/xonsh.git
            (7, 1123),  // https://github.com/meteor/meteor.git
            (8, 4),     // https://github.com/3b1b/manim.git
            (9, 2)      // https://github.com/s0md3v/photon.git
        );
}
