from urllib.parse import urlencode, urlparse, parse_qsl
import requests

api_key = "AIzaSyB5t7hSzpEmx1hk6clwou9wdQCpIvpDvP8"

data_type = 'json'



def extract_lat_lng(address_or_postalcode, data_type = 'json'):
    endpoint = f"https://maps.googleapis.com/maps/api/geocode/{data_type}"
    params= {"address": address_or_postalcode, "key": api_key}
    url_params = urlencode(params)
    url = f"{endpoint}?{url_params}"
    r = requests.get(url)
    latlng = {}
    if r.status_code not in range(200, 299):
        return {}
    try:
        latlng = r.json()['results'][0]['geometry']['location'] 
    except:
        pass
    return latlng.get("lat"), latlng.get("lng")

    

def get_route(start, end, data_type = 'json'):
    endpoint = f"https://maps.googleapis.com/maps/api/directions/{data_type}"
    params= {"destination": start, "origin": end, "key": api_key}
    url_params = urlencode(params)
    url = f"{endpoint}?{url_params}"
    r = requests.get(url)
    result = {}
    if r.status_code not in range(200, 299):
        return {}
    try:
        result = r.json() 
    except:
        pass
    steps = result['routes'][0]['legs'][0]['steps']

    points = []

    for i in range(len(steps)):
        if (i == 0):
            points.append(steps[i]["start_location"])
        points.append(steps[i]["end_location"])

    return points

def indentation(n):
    return ''.join('\t' for i in range(n))

def point_gpx(point):
    return ''.join([
        indentation(3),
        '<trkpt lat="' + str(point['lat']) + '" lon="' + str(point['lng']) + '">\n',
        indentation(4),
        '<time>' + '2012-07-03T08:33:10Z' + '</time>\n',
        indentation(3),
        '</trkpt>'
    ]) + '\n'

def generate_gpx_file(start, end):
    points = get_route(start, end)

    segments = ''.join([point_gpx(point) for point in points])
    
    return ''.join([
        '<?xml version="1.0" encoding="UTF-8"?>\n',
        '<gpx xmlns="http://www.topografix.com/GPX/1/1">\n',
        indentation(1) + '<trk>\n',
        indentation(2) + '<trkseg>\n', 
        segments, 
        indentation(2) + '</trkseg>\n', 
        indentation(1) + '</trk>\n',
        '</gpx>\n'
    ])
    

#const segments = state.get('tracks').get('tracks').get(trackId).get('segments').toJS().map((segmentId, i) => {
#    const segment = state.get('tracks').get('segments').get(segmentId);
#    const identation = '\t'.repeat(3);
#    const points = segment.get('points').map((point) => {
#      return [
#        identation,
#        '<trkpt lat="' + point.get('lat') + '" lon="' + point.get('lon') + '">',
#        '<time>' + point.get('time').toISOString().replace(MS_REG, 'Z') + '</time>',
#        '</trkpt>'
#      ].join('');
#    }).toJS().join('\n');
#    return '\t\t<trkseg>\n' + points + '\n\t\t</trkseg>';
#  }).join('\n');
#  return [
#    '<?xml version="1.0" encoding="UTF-8"?>',
#    '<gpx xmlns="http://www.topografix.com/GPX/1/1">',
#    '\t<trk>', segments, '\t</trk>',
#    '</gpx>'
#  ].join('\n');

if __name__=="__main__":
    print(generate_gpx_file("38.735986,-9.138793","38.7233,-9.1489"))