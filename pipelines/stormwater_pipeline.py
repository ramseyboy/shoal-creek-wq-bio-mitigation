transform_crs_query = """
select UpdateGeometrySRID('staging', 'OpenChannel', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'Artificial Flow Path', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'Bridge Inlet', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'Combo Inlet', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'Curb Inlet', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'Discharge Pump', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'Drainage Pipe', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'GrateInlet', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'Header', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'Junction', 'wkb_geometry', 26914);
select UpdateGeometrySRID('staging', 'Linear Drain', 'wkb_geometry', 26914);
"""