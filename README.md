# 데이터 파이프라인 관리
## Intro 
데이터 소스에서 수집한 원시 데이터를 데이터 마트, 데이터 웨어하우스로 이전하고 지속적으로 배포하는 파이프라인을 설계합니다.
Dune API, Alchemy API를 활용하여 수집한 데이터를 전처리 및 데이터 마트와 웨어하우스 형태로 적재하고 Wash Trade를 감지하는 Task로 구성되어 있습니다.

## Requirements
```
pip install -r requirements.txt
```
## Directory 구성
```
├── flow
|   ├── mart
|   └── warehouse
└── module
    ├── mart
    ├── utils
    └── warehouse
```

## 사용 데이터
### API Link
1. Dune : [Dune API](https://dune.com/api)
  > 수집 데이터에 대한 간략한 설명 
2. Alchemy : [Alchemy](https://www.alchemy.com/)
  > 수집 데이터에 대한 간략한 설명  

### ETL
![데이터](https://github.com/our-ml-flow/data_pipeline/assets/66200628/ab7f025a-e86b-4cc7-baf0-105a6b0af042)

### ERD
![erd](https://github.com/our-ml-flow/data_pipeline/assets/66200628/caef4ce4-01d7-4e49-8fcd-7dc7d099d70a)

## 파이프라인 배포
생성한 Task들을 하나의 Flow로 묶어 Flow를 배포합니다.

1. Flow 파일 실행
```
python flow_file_name.py
```
2. Agent 실행
```
prefect agent start --work-queue "Agent-Name"
```

