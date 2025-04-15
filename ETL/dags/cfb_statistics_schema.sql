drop database if exists cfb_statistics;
create database cfb_statistics;
use cfb_statistics;

drop table if exists kicking_stat;
drop table if exists punting_stat;
drop table if exists scoring_stat;
drop table if exists passing_stat;
drop table if exists receiving_stat;
drop table if exists rushing_stat;
drop table if exists roster;
drop table if exists player;
drop table if exists team_offense;
drop table if exists team_defense;
drop table if exists team_special;
drop table if exists team;
drop table if exists college;
drop table if exists conference;
drop table if exists position;
drop table if exists class;

create table class(
	class_id int primary key auto_increment,
    class_abbr varchar(10) not null,
    class_name varchar(100) not null
);

create table `position`(
	position_id int primary key auto_increment,
    position_abbr varchar(10) not null,
    position_name varchar(100) not null
);

create table conference(
    conference_id int primary key auto_increment,
    conference_shorthand varchar(10) not null,
    conference_name varchar(100) not null
);

create table college(
    college_id int primary key auto_increment,
    college_name varchar(100) not null,
    college_mascot varchar(100) not null
);
create table team(
    team_id int primary key auto_increment,
    `year` int not null,
    college_id int not null,
    conference_id int not null,
    constraint fk_team_college_id
	    foreign key (college_id)
        references college(college_id),
    constraint fk_team_conference_id
	    foreign key (conference_id)
        references conference(conference_id)
);

create table player(
	player_id int primary key auto_increment,
    player_name varchar(255) not null,
    position_id int not null,
    constraint fk_player_position_id
		foreign key (position_id)
        references `position`(position_id)
);

create table roster(
	roster_id int primary key auto_increment,
    player_id int not null,
    class_id int not null,
    team_id int not null,
    constraint fk_roster_player_id
		foreign key (player_id)
        references player(player_id),
	constraint fk_roster_class_id
		foreign key (class_id)
        references class(class_id),
	constraint fk_roster_team_id
		foreign key (team_id)
        references team(team_id)
);

create table rushing_stat(
	rushing_stat_id int primary key auto_increment,
    roster_id int not null,
    games_played int not null,
    rushing_attempts int not null,
    rushing_yards int not null,
    rushing_yards_per_attempt decimal(10,1) not null,
    rushing_touchdowns int not null,
    rushing_yards_per_game decimal(10,1) not null,
    constraint fk_rushing_stat_roster_id
        foreign key (roster_id)
        references roster(roster_id)
);

create table receiving_stat(
    receiving_stat_id int primary key auto_increment,
    roster_id int not null,
    games_played int not null,
    receptions int not null,
    receiving_yards int not null,
    receiving_yards_per_reception decimal(10,1) not null,
    receiving_touchdowns int not null,
    receiving_yards_per_game decimal(10,1) not null,
    constraint fk_receiving_stat_roster_id
        foreign key (roster_id)
        references roster(roster_id)    
);

create table passing_stat(
    passing_stat_id int primary key auto_increment,
    roster_id int not null,
    games_played int not null,
    completions int not null,
    passing_attempts int not null,
    completion_percentage decimal(10,1) not null,
    passing_yards int not null,
    passing_touchdowns int not null,
    touchdown_percentage decimal(10,1) not null,
    interceptions int not null,
    interception_percentage decimal(10,1) not null,
    passing_yards_per_attempt decimal(10,1) not null,
    passing_yards_per_completion decimal(10,1) not null,
    passing_yards_per_game decimal(10,1) not null,
    passer_rating decimal(10,1) not null,
    constraint fk_passing_stat_roster_id
        foreign key (roster_id)
        references roster(roster_id)
);

create table scoring_stat(
    scoring_stat_id int primary key auto_increment,
    roster_id int not null,
    games_played int not null,
    rushing_touchdowns int not null,
    receiving_touchdowns int not null,
    punt_return_touchdowns int not null,
    kickoff_return_touchdowns int not null,
    fumble_recovery_touchdowns int not null,
    interception_return_touchdowns int not null,
    other_touchdowns int not null,
    total_touchdowns int not null,
    extra_points_made int not null,
    extra_points_attempted int not null,
    field_goals_made int not null,
    field_goals_attempted int not null,
    two_point_conversions_made int not null,
    safeties int not null,
    points_scored int not null,
    points_per_game decimal(10,1) not null,
    constraint fk_scoring_stat_roster_id
        foreign key (roster_id)
        references roster(roster_id)
);

create table punting_stat(
    punting_stat_id int primary key auto_increment,
    roster_id int not null,
    games_played int not null,
    punts int not null,
    punt_yards int not null,
    yards_per_punt decimal(10,1) not null,
    constraint fk_punting_stat_roster_id
        foreign key (roster_id)
        references roster(roster_id)
);

create table kicking_stat(
    kicking_stat_id int primary key auto_increment,
    roster_id int not null,
    games_played int not null,
    extra_points_made int not null,
    extra_points_attempted int not null,
    extra_point_percentage decimal(10,1) not null,
    field_goals_made int not null,
    field_goals_attempted int not null,
    field_goal_percentage decimal(10,1) not null,
    points_scored int not null,
    constraint fk_kicking_stat_roster_id
        foreign key (roster_id)
        references roster(roster_id)
);

create table team_special(
    team_special_id int primary key auto_increment,
    team_id int not null,
    games_played int not null,
    extra_points_made_per_game decimal(10,1) not null,
    extra_points_attempted_per_game decimal(10,1) not null,
    extra_point_percentage decimal(10,1) not null,
    field_goals_made_per_game decimal(10,1) not null,
    field_goals_attempted_per_game decimal(10,1) not null,
    field_goal_percentage decimal(10,1) not null,
    kicking_points_scored_per_game decimal(10,1) not null,
    punts_per_game decimal(10,1) not null,
    punt_yards_per_game decimal(10,1) not null,
    yards_per_punt_per_game decimal(10,1) not null,
    kickoff_returns_per_game decimal(10,1) not null,
    kickoff_return_yards_per_game decimal(10,1) not null,
    return_yards_per_kickoff_per_game decimal(10,1) not null,
    kickoff_return_touchdowns_per_game decimal(10,1) not null,
    punt_returns_per_game decimal(10,1) not null,
    punt_return_yards_per_game decimal(10,1) not null,
    return_yards_per_punt_per_game decimal(10,1) not null,
    punt_return_touchdowns_per_game decimal(10,1) not null,
    constraint fk_team_special_team_id
        foreign key (team_id)
        references team(team_id)
);

create table team_defense(
    team_defense_id int primary key auto_increment,
    team_id int not null,
    games_played int not null,
    opponent_points_per_game decimal(10,1) not null,
    opponent_completions_per_game decimal(10,1) not null,
    opponent_passing_attempts_per_game decimal(10,1) not null,
    opponent_completion_percentage_per_game decimal(10,1) not null,
    opponent_passing_yards_per_game decimal(10,1) not null,
    opponent_passing_touchdowns_per_game decimal(10,1) not null,
    opponent_rushing_attempts_per_game decimal(10,1) not null,
    opponent_rushing_yards_per_game decimal(10,1) not null,
    opponent_rushing_yards_per_attempt_per_game decimal(10,1) not null,
    opponent_rushing_touchdowns_per_game decimal(10,1) not null,
    opponent_total_plays_per_game decimal(10,1) not null,
    opponent_total_yards_per_game decimal(10,1) not null,
    opponent_yards_per_play_per_game decimal(10,1) not null,
    opponent_passing_first_downs_per_game decimal(10,1) not null,
    opponent_rushing_first_downs_per_game decimal(10,1) not null,
    opponent_penalty_first_downs_per_game decimal(10,1) not null,
    opponent_total_first_downs_per_game decimal(10,1) not null,
    opponent_penalties_per_game decimal(10,1) not null,
    opponent_penalty_yards_per_game decimal(10,1) not null,
    opponent_fumbles_lost_per_game decimal(10,1) not null,
    opponent_interceptions_per_game decimal(10,1) not null,
    opponent_turnovers_per_game decimal(10,1) not null,
    constraint fk_team_defense_team_id
        foreign key (team_id)
        references team(team_id)
);

create table team_offense(
    team_offense_id int primary key auto_increment,
    team_id int not null,
    games_played int not null,
    points_per_game decimal(10,1) not null,
    completions_per_game decimal(10,1) not null,
    passing_attempts_per_game decimal(10,1) not null,
    completion_percentage_per_game decimal(10,1) not null,
    passing_yards_per_game decimal(10,1) not null,
    passing_touchdowns_per_game decimal(10,1) not null,
    rushing_attempts_per_game decimal(10,1) not null,
    rushing_yards_per_game decimal(10,1) not null,
    rushing_yards_per_attempt_per_game decimal(10,1) not null,
    rushing_touchdowns_per_game decimal(10,1) not null,
    total_plays_per_game decimal(10,1) not null,
    total_yards_per_game decimal(10,1) not null,
    yards_per_play_per_game decimal(10,1) not null,
    passing_first_downs_per_game decimal(10,1) not null,
    rushing_first_downs_per_game decimal(10,1) not null,
    penalty_first_downs_per_game decimal(10,1) not null,
    total_first_downs_per_game decimal(10,1) not null,
    penalties_per_game decimal(10,1) not null,
    penalty_yards_per_game decimal(10,1) not null,
    fumbles_lost_per_game decimal(10,1) not null,
    interceptions_per_game decimal(10,1) not null,
    turnovers_per_game decimal(10,1) not null,
    constraint fk_team_offense_team_id
        foreign key (team_id)
        references team(team_id)
);