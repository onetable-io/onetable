/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

module.exports = {
    docs: [
        "setup",
        "features-and-limitations",
        {
            type: 'category',
            label: 'Quick Start',
            collapsed: false,
            items: [
                'how-to'
            ],
        },
        {
            type: 'category',
            label: 'Integrations',
            collapsed: false,
            items: [
                {
                    type: 'category',
                    label: 'Catalogs',
                    collapsed: true,
                    link: {
                        type: 'doc',
                        id: 'catalogs-index'
                    },
                    items: [
                        'hms',
                        'glue-catalog',
                        'unity-catalog',
                        'biglake-metastore',
                    ],
                },
                {
                    type: 'category',
                    label: 'Query Engines',
                    collapsed: true,
                    link: {
                        type: 'doc',
                        id: 'query-engines-index'
                    },
                    items: [
                        'athena',
                        'redshift',
                        'spark',
                        'bigquery',
                        'fabric',
                        'presto',
                        'snowflake',
                        'trino',
                    ],
                }
            ]
        },
        {
            type: 'category',
            label: 'Docker',
            collapsed: false,
            items: [
                'docker/demo',
                'docker/playground',
            ],
        }
    ],
};