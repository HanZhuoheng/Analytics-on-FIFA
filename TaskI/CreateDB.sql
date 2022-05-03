DROP TABLE IF EXISTS FIFA.NAMES CASCADE;
DROP TABLE IF EXISTS FIFA.OVERALL_ATTRIBUTES CASCADE;
DROP TABLE IF EXISTS FIFA.ATTACKING_ATTRIBUTES CASCADE;
DROP TABLE IF EXISTS FIFA.SKILL_ATTRIBUTES CASCADE;
DROP TABLE IF EXISTS FIFA.MOVEMENT_ATTRIBUTES CASCADE;
DROP TABLE IF EXISTS FIFA.POWER_ATTRIBUTES CASCADE;
DROP TABLE IF EXISTS FIFA.MENTALITY_ATTRIBUTES CASCADE;
DROP TABLE IF EXISTS FIFA.DEFENDING_ATTRIBUTES CASCADE;
DROP TABLE IF EXISTS FIFA.GOALKEEPING_ATTRIBUTES CASCADE;
DROP TABLE IF EXISTS FIFA.OTHER_ATTRIBUTES CASCADE;
DROP TABLE IF EXISTS FIFA.PLAYERS CASCADE;

CREATE TABLE IF NOT EXISTS FIFA.NAMES(
	Short_Name VARCHAR(100) PRIMARY KEY,
	Long_Name VARCHAR(100));

CREATE TABLE IF NOT EXISTS FIFA.OVERALL_ATTRIBUTES(
	attribute_id SMALLINT PRIMARY KEY,
	overall SMALLINT, 
    potential SMALLINT, 
	pace SMALLINT, 
    shooting SMALLINT, 
    passing SMALLINT, 
    dribbling SMALLINT, 
    defending SMALLINT, 
    physic SMALLINT);
	
CREATE TABLE IF NOT EXISTS FIFA.ATTACKING_ATTRIBUTES(
	attacking_id SMALLINT PRIMARY KEY,
	attacking_crossing SMALLINT, 
    attacking_finishing SMALLINT, 
    attacking_heading_accuracy SMALLINT,
    attacking_short_passing SMALLINT, 
    attacking_volleys SMALLINT);

CREATE TABLE IF NOT EXISTS FIFA.SKILL_ATTRIBUTES(
	skill_id SMALLINT PRIMARY KEY,
	skill_dribbling SMALLINT, 
    skill_curve SMALLINT, 
    skill_fk_accuracy SMALLINT, 
    skill_long_passing SMALLINT, 
    skill_ball_control SMALLINT);
	
CREATE TABLE IF NOT EXISTS FIFA.MOVEMENT_ATTRIBUTES(
	movement_id SMALLINT PRIMARY KEY,
	movement_acceleration SMALLINT, 
    movement_sprint_speed SMALLINT, 
    movement_agility SMALLINT, 
    movement_reactions SMALLINT, 
    movement_balance SMALLINT);	
	
CREATE TABLE IF NOT EXISTS FIFA.POWER_ATTRIBUTES(
	power_id SMALLINT PRIMARY KEY,
	power_shot_power SMALLINT, 
    power_jumping SMALLINT, 
    power_stamina SMALLINT, 
    power_strength SMALLINT, 
    power_long_shots SMALLINT);	

CREATE TABLE IF NOT EXISTS FIFA.MENTALITY_ATTRIBUTES(
	mentality_id SMALLINT PRIMARY KEY,
	mentality_aggression SMALLINT, 
    mentality_interceptions SMALLINT, 
    mentality_positioning SMALLINT,
    mentality_vision SMALLINT, 
    mentality_penalties SMALLINT);	

CREATE TABLE IF NOT EXISTS FIFA.DEFENDING_ATTRIBUTES(
	defending_id SMALLINT PRIMARY KEY,
    defending_marking SMALLINT, 
    defending_standing_tackle SMALLINT, 
    defending_sliding_tackle SMALLINT);

CREATE TABLE IF NOT EXISTS FIFA.GOALKEEPING_ATTRIBUTES(
	goalkeeping_id SMALLINT PRIMARY KEY,
	goalkeeping_diving SMALLINT, 
    goalkeeping_handling SMALLINT, 
    goalkeeping_kicking SMALLINT, 
    goalkeeping_positioning SMALLINT, 
    goalkeeping_reflexes SMALLINT);

CREATE TABLE IF NOT EXISTS FIFA.OTHER_ATTRIBUTES(
	other_id SMALLINT PRIMARY KEY,
	ls SMALLINT, 
    st SMALLINT, 
    rs SMALLINT, 
    lw SMALLINT, 
    lf SMALLINT, 
    cf SMALLINT, 
    rf SMALLINT, 
    rw SMALLINT, 
    lam SMALLINT, 
    cam SMALLINT, 
    ram SMALLINT, 
    lm SMALLINT, 
    lcm SMALLINT, 
    cm SMALLINT, 
    rcm SMALLINT, 
    rm SMALLINT, 
    lwb SMALLINT, 
    ldm SMALLINT, 
    cdm SMALLINT, 
    rdm SMALLINT, 
    rwb SMALLINT, 
    lb SMALLINT, 
    lcb SMALLINT, 
    cb SMALLINT, 
    rcb SMALLINT, 
    rb SMALLINT);

CREATE TABLE IF NOT EXISTS FIFA.PLAYERS(
	sofifa_id SERIAL PRIMARY KEY, 
    player_url VARCHAR(1000), 
    name VARCHAR(100) references FIFA.NAMES,
	age SMALLINT, 
    dob DATE, 
    height_cm SMALLINT, 
    weight_kg SMALLINT, 
    nationality VARCHAR(100), 
    club VARCHAR(100),
    value_eur SMALLINT, 
    wage_eur SMALLINT, 
    player_positions VARCHAR(100), 
    preferred_foot VARCHAR(100), 
    international_reputation SMALLINT, 
    weak_foot SMALLINT, 
    skill_moves SMALLINT, 
    work_rate VARCHAR(100), 
    body_type VARCHAR(100), 
    real_face VARCHAR(100), 
    team_position VARCHAR(10), 
    team_jersey_number SMALLINT, 
    joined DATE, 
    contract_valid_until VARCHAR(5),
	nation_position VARCHAR(10),
	nation_jersey_number SMALLINT,
	attribute_id SMALLINT REFERENCES FIFA.OVERALL_ATTRIBUTES,
	attacking_id SMALLINT REFERENCES FIFA.ATTACKING_ATTRIBUTES,
	skill_id SMALLINT REFERENCES FIFA.SKILL_ATTRIBUTES,
	movement_id SMALLINT REFERENCES FIFA.MOVEMENT_ATTRIBUTES,
	power_id SMALLINT REFERENCES FIFA.POWER_ATTRIBUTES,
	mentality_id SMALLINT REFERENCES FIFA.MENTALITY_ATTRIBUTES,
	defending_id SMALLINT REFERENCES FIFA.DEFENDING_ATTRIBUTES,
	goalkeeping_id SMALLINT REFERENCES FIFA.GOALKEEPING_ATTRIBUTES,
	other_id SMALLINT REFERENCES FIFA.OTHER_ATTRIBUTES,
	year VARCHAR(5)); 

