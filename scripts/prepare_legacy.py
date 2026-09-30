"""원본을 보존하며 로컬 실행에 필요한 변경을 반영한 사본 생성."""
from pathlib import Path
import hashlib
import json
ROOT=Path(__file__).resolve().parents[1]

def main():
    dest=ROOT/'compatible'; dest.mkdir(exist_ok=True)
    names=['Feature_Engineering_(AI).ipynb','W07_Spark_Structured_Streaming.ipynb','week09_ctr_multi_objective_optimization_notebook.ipynb']
    manifest={}
    for name in names:
        original=ROOT/name
        manifest[name]=hashlib.sha256(original.read_bytes()).hexdigest()
        nb=json.loads(original.read_text(encoding='utf-8'))
        nb['nbformat']=4
        nb['nbformat_minor']=5
        for cell in nb['cells']:
            if cell['cell_type']!='code': continue
            source=''.join(cell['source'])
            if name.startswith('Feature'):
                source=source.replace("file_path = '/content/sample_data/california_housing_train.csv'", "import os\nfrom pathlib import Path\nfile_path = os.environ.get('HOUSING_CSV', '/content/sample_data/california_housing_train.csv')\nif not Path(file_path).is_file():\n    raise FileNotFoundError('HOUSING_CSV에 California Housing CSV 경로를 지정하세요. COMPATIBILITY.md 참고.')")
                source=source.replace('n_iter=250','max_iter=250')
            if name.startswith('W07'):
                source=source.replace('!pip install pyspark kafka-python','!pip install pyspark==3.5.3 kafka-python==2.0.2')
                source=source.replace('org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3')
                source=source.replace('# Spark 4.0.x 버전에 맞는 Kafka 커넥터 포함 (Scala 2.13 기반)', '# PySpark 3.5.3 / Scala 2.12와 Kafka 커넥터 버전 일치')
                source=source.replace('# 현재 PySpark 4.0.2가 설치되어 있으므로 Spark 4.0.x용 커넥터를 사용합니다.', '# 설치 버전을 바꾸면 런타임을 재시작한 뒤 세션을 생성하세요.')
            cell['source']=source.splitlines(True); cell['execution_count']=None; cell['outputs']=[]
        note='원본 보존용 호환 사본. 상위 폴더 COMPATIBILITY.md를 먼저 확인하세요. '
        if name.startswith('W07'): note+='Kafka 설치 셀은 Linux/Colab 전용입니다. Windows 기본 실습은 week07_structured_streaming_lab.ipynb를 사용하세요.'
        if name.startswith('Feature'): note+='회귀 트리의 기본 중요도는 분산/MSE 감소량입니다. 원문의 Gini/Information Gain 표제를 회귀 결과의 지표 이름으로 해석하지 마세요.'
        nb['cells'].insert(0,{'cell_type':'markdown','metadata':{},'source':['# 호환 사본 안내\n',note]})
        for i,cell in enumerate(nb['cells']): cell['id']=f'legacy-{i:03}'
        (dest/name).write_text(json.dumps(nb,ensure_ascii=False,indent=1),encoding='utf-8')
    (dest/'original_sha256.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print('Created 3 compatibility copies; original files unchanged.')
if __name__=='__main__': main()
