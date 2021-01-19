use std::collections::*;
use std::iter::*;

use chrono::DateTime;
use parasite::DatastoreView;

use crate::Djanco;
use crate::data::*;
use crate::objects::*;

fn database() -> Database {
    //let store = DatastoreView::new("/dejacode/testing/10b", now.into());
    let database = Djanco::from_store("/dejacode/testing/10b", timestamp!(December 2020), vec![]).unwrap();

    database
}

macro_rules! check_project_attrib_value {
        ($method:ident -> $type:ty, $converter:expr, $($values:expr),+) => {{
            let database = database();

            let expected: BTreeMap<ProjectId, $type> = BTreeMap::from_iter(vec![
                $($values,)+
            ].into_iter().map(|(i, e) | (ProjectId::from(i as usize), $converter(e))));

            let actual: BTreeMap<ProjectId, $type> =
                BTreeMap::from_iter(database.projects().map(|p| (p.id(), p.$method())));

            assert_eq!(expected, actual);
        }}
    }

#[test] fn project_urls () {
    check_project_attrib_value!(url -> String, |e: &str| e.to_owned(),
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
    check_project_attrib_value!(language -> Option<Language>, |e: Language| Some(e),
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
    check_project_attrib_value!(star_count -> Option<usize>, |e: usize| Some(e),
            (0, 75415),     //https://github.com/nodejs/node.git
            (1, 31403),     //https://github.com/pixijs/pixi.js.git
            (2, 68799),     //https://github.com/angular/angular.git
            (3, 19612),     //https://github.com/apache/airflow.git
            (4, 160740),    //https://github.com/facebook/react.git
            (5, 176839),    //https://github.com/vuejs/vue.git
            (6, 4117),      //https://github.com/xonsh/xonsh.git
            (7, 42118),     //https://github.com/meteor/meteor.git
            (8, 29000),     //https://github.com/3b1b/manim.git
            (9, 7398)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_watchers() {
    check_project_attrib_value!(watcher_count -> Option<usize>, |e: usize| Some(e),
            (0, 75415),     //https://github.com/nodejs/node.git
            (1, 31403),     //https://github.com/pixijs/pixi.js.git
            (2, 68799),     //https://github.com/angular/angular.git
            (3, 19612),     //https://github.com/apache/airflow.git
            (4, 160740),    //https://github.com/facebook/react.git
            (5, 176839),    //https://github.com/vuejs/vue.git
            (6, 4117),      //https://github.com/xonsh/xonsh.git
            (7, 42118),     //https://github.com/meteor/meteor.git
            (8, 29000),     //https://github.com/3b1b/manim.git
            (9, 7398)       //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_subscribers() {
    check_project_attrib_value!(subscriber_count -> Option<usize>, |e: usize| Some(e),
            (0, 2985),    //https://github.com/nodejs/node.git
            (1, 1056),    //https://github.com/pixijs/pixi.js.git
            (2, 3212),    //https://github.com/angular/angular.git
            (3, 737),     //https://github.com/apache/airflow.git
            (4, 6749),    //https://github.com/facebook/react.git
            (5, 6350),    //https://github.com/vuejs/vue.git
            (6, 99),      //https://github.com/xonsh/xonsh.git
            (7, 1715),    //https://github.com/meteor/meteor.git
            (8, 791),     //https://github.com/3b1b/manim.git
            (9, 291)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_forks() {
    check_project_attrib_value!(fork_count -> Option<usize>, |e: usize| Some(e),
            (0, 18814),    //https://github.com/nodejs/node.git
            (1, 4218),     //https://github.com/pixijs/pixi.js.git
            (2, 18195),    //https://github.com/angular/angular.git
            (3, 7630),     //https://github.com/apache/airflow.git
            (4, 32006),    //https://github.com/facebook/react.git
            (5, 27556),    //https://github.com/vuejs/vue.git
            (6, 446),      //https://github.com/xonsh/xonsh.git
            (7, 5154),     //https://github.com/meteor/meteor.git
            (8, 3699),     //https://github.com/3b1b/manim.git
            (9, 1029)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_open_issues() {
    check_project_attrib_value!(open_issue_count -> Option<usize>, |e: usize| Some(e),
            (0, 1237),    //https://github.com/nodejs/node.git
            (1, 66),      //https://github.com/pixijs/pixi.js.git
            (2, 2838),    //https://github.com/angular/angular.git
            (3, 923),     //https://github.com/apache/airflow.git
            (4, 682),     //https://github.com/facebook/react.git
            (5, 551),     //https://github.com/vuejs/vue.git
            (6, 418),     //https://github.com/xonsh/xonsh.git
            (7, 131),     //https://github.com/meteor/meteor.git
            (8, 331),     //https://github.com/3b1b/manim.git
            (9, 36)       //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_sizes() {
    check_project_attrib_value!(size -> Option<usize>, |e: usize| Some(e),
            (0, 639607),    //https://github.com/nodejs/node.git
            (1, 72165),     //https://github.com/pixijs/pixi.js.git
            (2, 271123),    //https://github.com/angular/angular.git
            (3, 97275),     //https://github.com/apache/airflow.git
            (4, 161343),    //https://github.com/facebook/react.git
            (5, 27591),     //https://github.com/vuejs/vue.git
            (6, 23304),     //https://github.com/xonsh/xonsh.git
            (7, 80487),     //https://github.com/meteor/meteor.git
            (8, 17759),     //https://github.com/3b1b/manim.git
            (9, 356)        //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_is_fork() {
    check_project_attrib_value!(is_fork -> Option<bool>, |e: bool| Some(e),
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
    check_project_attrib_value!(is_archived -> Option<bool>, |e: bool| Some(e),
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
    check_project_attrib_value!(is_disabled -> Option<bool>, |e: bool| Some(e),
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
    check_project_attrib_value!(has_pages -> Option<bool>, |e: bool| Some(e),
            (0, false),     //https://github.com/nodejs/node.git
            (1, false),     //https://github.com/pixijs/pixi.js.git
            (2, false),     //https://github.com/angular/angular.git
            (3, false),     //https://github.com/apache/airflow.git
            (4, true),      //https://github.com/facebook/react.git
            (5, false),     //https://github.com/vuejs/vue.git
            (6, false),     //https://github.com/xonsh/xonsh.git
            (7, false),     //https://github.com/meteor/meteor.git
            (8, false),     //https://github.com/3b1b/manim.git
            (9, false)      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_has_downloads() {
    check_project_attrib_value!(has_downloads -> Option<bool>, |e: bool| Some(e),
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
    check_project_attrib_value!(has_wiki -> Option<bool>, |e: bool| Some(e),
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
    check_project_attrib_value!(has_issues -> Option<bool>, |e: bool| Some(e),
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
    check_project_attrib_value!(default_branch -> Option<String>, |e: &str| Some(e.to_owned()),
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
    check_project_attrib_value!(license -> Option<String>, |e: &str| Some(e.to_owned()),
            (0, "Other"),                               //https://github.com/nodejs/node.git
            (1, "MIT License"),                         //https://github.com/pixijs/pixi.js.git
            (2, "MIT License"),                         //https://github.com/angular/angular.git
            (3, "Apache License 2.0"),                  //https://github.com/apache/airflow.git
            (4, "MIT License"),                         //https://github.com/facebook/react.git
            (5, "MIT License"),                         //https://github.com/vuejs/vue.git
            (6, "Other"),                               //https://github.com/xonsh/xonsh.git
            (7, "Other"),                               //https://github.com/meteor/meteor.git
            (8, "Other"),                               //https://github.com/3b1b/manim.git
            (9, "GNU General Public License v3.0")      //https://github.com/s0md3v/photon.git
        );
}

#[test] fn project_description() {
    check_project_attrib_value!(description -> Option<String>, |e: &str| Some(e.to_owned()),
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

#[test] fn project_homepage() {
    check_project_attrib_value!(homepage -> Option<String>, |e: Option<&str>| e.map(|e: &str| e.to_owned()),
            (0, Some("https://nodejs.org/")),
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

#[test] fn project_created() {
    check_project_attrib_value!(created -> Option<i64>, |e: &str| Some(DateTime::parse_from_rfc3339(e).unwrap().timestamp()),
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

#[test] fn project_updated() {
    check_project_attrib_value!(updated -> Option<i64>, |e: &str| Some(DateTime::parse_from_rfc3339(e).unwrap().timestamp()),
            (8, "2020-12-17T17:12:22Z"),
            (6, "2020-12-17T16:53:36Z"),
            (1, "2020-12-17T17:20:36Z"),
            (9, "2020-12-17T15:01:36Z"),
            (3, "2020-12-17T15:01:21Z"),
            (4, "2020-12-17T17:17:19Z"),
            (2, "2020-12-17T17:00:56Z"),
            (7, "2020-12-17T11:08:27Z"),
            (0, "2020-12-17T17:12:19Z"),
            (5, "2020-12-17T17:17:25Z")
        );
}

#[test] fn project_pushed() {
    check_project_attrib_value!(pushed -> Option<i64>, |e: &str| Some(DateTime::parse_from_rfc3339(e).unwrap().timestamp()),
            (8, "2020-12-04T16:26:20Z"),
            (6, "2020-12-17T16:19:16Z"),
            (1, "2020-12-17T17:10:44Z"),
            (9, "2020-10-28T14:01:02Z"),
            (3, "2020-12-17T17:19:24Z"),
            (4, "2020-12-17T17:05:15Z"),
            (2, "2020-12-17T17:01:19Z"),
            (7, "2020-12-11T12:26:58Z"),
            (0, "2020-12-17T16:03:55Z"),
            (5, "2020-12-17T13:27:21Z")
        );
}

#[test] fn project_head_count() {
    check_project_attrib_value!(head_count -> Option<usize>, |e: usize| Some(e),
            (0, 33),
            (1, 29),
            (2, 57),
            (3, 15),
            (4, 102),
            (5, 61),
            (6, 42),
            (7, 1110),
            (8, 16),
            (9, 2)
        );
}

// #[test] fn project_heads() {
//     check_project_attrib_value!(head_count -> Option<Vec<Head>>, |e: usize| Some(e),
//         (0, vec![]),
//         (1, vec![]),
//         (2, vec![]),
//         (3, vec![]),
//         (4, vec![]),
//         (5, vec![]),
//         (6, vec![]),
//         (7, vec![]),
//         (8, vec![]),
//         (9, vec!["master", "2.0-beta"])
//     );
// }