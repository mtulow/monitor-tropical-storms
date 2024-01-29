import pandas as pd
import geopandas as gpd
from pprint import pprint
from osgeo import gdal, ogr
from collections import defaultdict

def analyze_geometry(geometry: ogr.Geometry, indent: int = 0):
    s = []
    s.append(' '.center(indent), )
    s.append(geometry.GetGeometryName())

    point_count = geometry.GetPointCount()
    if point_count > 0:
        s.append(f' with {point_count} data points')
    
    geometry_count = geometry.GetGeometryCount()
    if geometry_count > 0:
        s.append(' containing:')

    print(''.join(s))

    for i in range(geometry_count):
        analyze_geometry(geometry.GetGeometryRef(i), indent + 4)
        
        
def summarize_shapefile(file_path: str):
    shapefile = ogr.Open(file_path)
    n_layers = shapefile.GetLayerCount()

    print(f"Number of layers: {n_layers}")
    print(F'shapefile contains {n_layers} layers\n')
    for i in range(n_layers):
        layer = shapefile.GetLayer(i)
        spatial_ref = layer.GetSpatialRef().ExportToProj4()
        n_features = layer.GetFeatureCount()
        
        print(f'Layer {i} has spatial reference: {spatial_ref}')
        print(f'Layer {i} has {n_features} features\n')

        for feature_n in range(n_features):
            feature = layer.GetFeature(feature_n)
            feature_name = feature.GetField('Name')
            print(f'Feature {feature_n} has name {feature_name}')

def get_row(shapefile: ogr.DataSource, layer_idx: int, feature_idx: int):
    n_layers = shapefile.GetLayerCount()

    for i in range(n_layers):
        layer = shapefile.GetLayer(i)
        n_features = layer.GetFeatureCount()


    layer = shapefile.GetLayer(layer_idx)
    feature = layer.GetFeature(feature_idx)
    attributes = feature.items()

    return attributes


if __name__ == '__main__':
    print()
    
    file_path = 'data/IBTrACS/shapefile/active/IBTrACS.ACTIVE.list.v04r00.points.shp'
    
    shapefile = ogr.Open(file_path)
    layer = shapefile.GetLayer(0)
    feature = layer.GetFeature(2)
    geometry = feature.GetGeometryRef()
    analyze_geometry(geometry)
    
    print()