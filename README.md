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

## 데이터 수집
### Dune API
Link : [Dune API](https://dune.com/api)

> 수집 데이터 내역
  -
  - 

### Alchemy
Link : [Alchemy](https://www.alchemy.com/)

> 수집 데이터 내역
  -
  - 
