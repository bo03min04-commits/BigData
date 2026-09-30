"""독립 Python 네임스페이스에서 새 실습 셀을 순서대로 실행한다."""
import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import time
import traceback
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--spark', action='store_true')
    parser.add_argument('--week', type=int)
    parser.add_argument('--report',type=Path,default=ROOT/'outputs/verification.json')
    args=parser.parse_args()
    os.chdir(ROOT)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import nbformat
    report=[]
    for path in sorted(ROOT.glob('week*_lab.ipynb')):
        nb=nbformat.read(path,as_version=4); nbformat.validate(nb)
        meta=nb.metadata.bigdata_lab
        if args.week and meta.week!=args.week: continue
        if meta.requires_spark and not args.spark: continue
        print('RUN',path.name,flush=True)
        start=time.perf_counter(); capture=io.StringIO()
        namespace={'__name__':'__main__','display':lambda x: print(x)}
        status='passed'; error=None
        try:
            with contextlib.redirect_stdout(capture):
                for index,cell in enumerate(nb.cells):
                    if cell.cell_type=='code':
                        exec(compile(cell.source,f'{path.name}:cell{index}','exec'),namespace)
        except Exception:
            status='failed'; error=traceback.format_exc(); print(error,flush=True)
        finally:
            if 'spark' in namespace:
                try: namespace['spark'].stop()
                except Exception: pass
            plt.close('all')
        args.report.parent.mkdir(parents=True,exist_ok=True)
        (args.report.parent/(path.stem+'.log')).write_text(capture.getvalue()+(error or ''),encoding='utf-8')
        report.append({'notebook':path.name,'status':status,'seconds':time.perf_counter()-start,'error':error})
        print(status,flush=True)
    args.report.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    if any(r['status']=='failed' for r in report): raise SystemExit(1)

if __name__=='__main__': main()
