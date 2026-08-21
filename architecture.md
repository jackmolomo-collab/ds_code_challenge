ds_code_challenge/
│
├── docker-compose.yml
├── .env.example
├── README.md
├── AI.md                              # AI usage & prompt audit log
│
├── ai/                                # AI-assisted development assets
│   ├── prompts/
│   │   ├── architecture/
│   │   ├── ingestion/
│   │   ├── airflow/
│   │   ├── transformation/
│   │   ├── data_quality/
│   │   └── testing/
│   │
│   └── README.md
│
├── dags/
│   ├── ingestion/
│   │   └── ingestion_dag.py
│   │
│   ├── transformation/
│   │   └── transformation_dag.py
│   │
│   ├── quality/
│   │   └── data_quality_dag.py
│   │
│   └── master/
│       └── data_pipeline_dag.py
│
├── src/
│   ├── ingestion/
│   │   └── ingest.py
│   │
│   ├── transformation/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   │
│   ├── quality/
│   │   └── checks.py
│   │
│   ├── storage/
│   │   └── s3.py
│   │
│   └── common/
│       ├── config.py
│       └── logging.py
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_transformation.py
│   └── test_quality.py
│
├── sql/
│   ├── silver/
│   └── gold/
│
├── config/
│   └── pipeline.yaml
│
├── data/
│   └── sample/
│
└── docs/
    ├── architecture.md
    └── data_dictionary.md

I will add the airflow bot for checking jobs mini ai chat