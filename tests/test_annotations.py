from obiba_opal.export_annotations import ExportAnnotationsService
from tests.utils import make_client
import io

def test_variable_annotations():
    client = make_client()
    output = io.StringIO()
    ExportAnnotationsService(client).export_variable_annotations('CLSA', 'Tracking_60min_R1', 'WGHTS_PROV_TRM', output, taxonomies=['Mlstr_area'])
    rows = output.getvalue().split('\r\n')
    rows = [line.split('\t') for line in rows if len(line) > 0]
    assert len(rows) == 3
    assert len(rows[0]) == 6
    row = rows[2]
    assert row[0] == 'CLSA'
    assert row[1] == 'Tracking_60min_R1'
    assert row[2] == 'WGHTS_PROV_TRM'
    assert row[3] == 'Mlstr_area'
    