drop database if exists cfb_statistics;
create database cfb_statistics;
use cfb_statistics;

drop table if exists rushing_stat;
drop table if exists roster;
drop table if exists player;
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
    rush_attempts int not null,
    rush_yards int not null,
    rush_yards_per_att decimal(10,1) not null,
    rush_touchdowns int not null,
    rush_yards_per_game decimal(10,1) not null
);