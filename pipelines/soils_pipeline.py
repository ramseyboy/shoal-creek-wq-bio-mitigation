query = """
create table public.soils
as 
select
             s."OBJECTID" as fid,
             s."Shape" as geometry,
             s.compname as soil_name, 
             s.taxclname as soil_taxonomy, 
             s.taxorder as soil_order,
             s.taxpartsiz as particle_size,
             s.hydgrpdcd as hydrologic_group, 
             s.earthcovki as earth_cover, 
             s.runoff as runoff_class, 
             s.slopegradw as avg_slope_gradient,
             s.brockdepmi as min_bedrock_depth,
             s.wtdepannmi as min_water_table_depth,
             s.forpehrtdc as potential_erosion_hazard, 
             s.erocl as erosion_class,
             s.earthcovki as earth_cover_kind_1,
             s.earthcov_1 as earth_cover_kind_2,
             s.geomdesc as geomorphic_description,
             s.aspectrep as aspect,
             s.drclassdcd as drainage_class_dominant, 
             s.drclasswet as drainage_class_wettest, 
             s.reannualpr as annual_precipitation, 
             s.flodfreqdc as flood_prob_annual 
    FROM staging."SoilTypes_TC" as s

"""