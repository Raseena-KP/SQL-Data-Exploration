--select query
select *
from CovidDeaths$

--look for average population,new cases & deaths vs continent & location
select continent,location,avg(population) as avg_population,avg(new_cases) as avg_new,avg(new_deaths) as avg_death
from CovidDeaths$
group by continent,location
order by continent desc

--continents vs avg_population
select continent,avg(population) as avg_population
from CovidDeaths$
group by continent
order by continent desc

--continent vs total_deaths
select distinct continent,avg(total_deaths)
from CovidDeaths$
group by continent
order by continent desc

select *
from CovidDeaths$
order by 3,4

--likelyhood of dying in countries,having 'states' in their name
select location,date,total_cases,total_deaths,(total_deaths/total_cases)*100 as death_percentage
from CovidDeaths$
where location like '%states%' and total_deaths>=1
order by 1,2

--change data type of the column using cast
select location,date,total_cases,total_deaths,(cast (total_deaths as float)/cast (total_cases as float))*100 as death_percentage
from CovidDeaths$
where location like '%states%' and total_deaths>=1
order by 5 desc

--likelyhood of dying vs continent
select continent,avg(population) as avg_pop, avg ((cast (total_deaths as float)/cast (total_cases as float))*100) as death_percentage
from CovidDeaths$
where total_deaths>=1
group by continent
order by 3

--looking at lotal cases vs population
select location,date,population, total_cases,(cast (total_cases as float)/cast (population as float))*100 as death_percentage
from CovidDeaths$
order by 5 desc
---------------------------------------------------------------------------------
--looking at the contries with highest infection rate compared to population
select location, Max(cast (total_tests as float)) as HighestInfectionCount,max(((cast (total_tests as float))/population)*100) as Percent_population_infected
from CovidDeaths$
where  total_tests<>''
group by location
order by 3 desc


select location,population,MAX(total_cases) as HighestInfection_Count,max((total_cases/population))*100 as PercentPopulation_Infected
from CovidDeaths$
where continent is not null
group by location,population
order by 4 desc
---------------------------------------------------------------------------------
--showing the contries with highest Death count per population
select location,MAX(cast(total_deaths as int)) as HighestDeath_Count,max((cast(total_deaths as int))/population)*100 as PercentPopulation_died
from CovidDeaths$
where continent is not null
group by location
order by 3 desc

select *
from CovidDeaths$
where continent is not null

select location,MAX(cast(total_deaths as int)) as HighestDeath_Count
from CovidDeaths$
where continent is not null
group by location
order by 2 desc

---------------------------------------------------------------------------------
--Lets break things down for continent
--Showing continents with highest death count
select continent,MAX(cast(total_deaths as int)) as HighestDeath_Count
from CovidDeaths$
where continent is not null
group by continent
order by 2 desc
---------------------------------------------------------------------------------
--we want to calculate everything for entire world
--GLOBAL NUMBERS
select location,date,total_cases,total_deaths,(cast(total_deaths as int)/cast(total_cases as int))*100 as death_percentage
from CovidDeaths$
where continent is not null
order by 1,2


select date,sum(new_cases) as total_cases,sum(cast(new_deaths as int)) as total_deaths,(sum(cast(new_deaths as int))/sum(new_cases))*100 as new_rat--,total_deaths,(cast(total_deaths as int)/cast(total_cases as int))*100 as death_percentage
from CovidDeaths$
where continent is not null and new_cases>0
group by date
order by 1,2

select sum(new_cases) as total_cases,sum(cast(new_deaths as int)) as total_deaths,(sum(cast(new_deaths as int))/sum(new_cases))*100 as new_rat--,total_deaths,(cast(total_deaths as int)/cast(total_cases as int))*100 as death_percentage
from CovidDeaths$
where continent is not null and new_cases>0
order by 1,2

select *
from CovidVaccination$
---------------------------------------------------------------------------------
-- join covid_deaths & vaccination tables based on location & date
select *
from CovidDeaths$ as dea
JOIN CovidVaccination$ as vac
	on dea.location=vac.location 
	and dea.date=vac.date
---------------------------------------------------------------------------------
--Looking at total population vs vaccination

select dea.location,max(population) as highest_population,max(people_fully_vaccinated) as vaccination_count
from CovidDeaths$ as dea
JOIN CovidVaccination$ as vac
	on dea.location=vac.location 
	and dea.date=vac.date
group by dea.location


select dea.continent,dea.location,dea.date,population,vac.new_vaccinations
from CovidDeaths$ as dea
JOIN CovidVaccination$ as vac
	on dea.location=vac.location 
	and dea.date=vac.date
where dea.continent is not null
order by 2,3
---------------------------------------------------------------------------------
--add a running total for each continent using partition by
select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations
,sum(cast(vac.new_vaccinations as bigint)) over(partition by dea.location order by dea.location,dea.date) as RollingPeople_vaccinated
from CovidDeaths$ as dea
JOIN CovidVaccination$ as vac
	on dea.location=vac.location 
	and dea.date=vac.date
where dea.continent is not null
order by 2,3
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
--now we need to do some calculation on newly created rows
--for that purpose we need to go for cte or temp table

---------------------------------------------------------------------------------
--USE CTE
--no of columns in CTE should be same as the fields in the Select query
--dont include any 'order by' statement
--select statement need to run with cte command

with popvsvac (continent,location,date,population,new_vaccinations,RollingPeople_vaccinated)
as
(
select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations
,sum(cast(vac.new_vaccinations as bigint)) over(partition by dea.location order by dea.location,dea.date) as RollingPeople_vaccinated
from CovidDeaths$ as dea
JOIN CovidVaccination$ as vac
	on dea.location=vac.location 
	and dea.date=vac.date
where dea.continent is not null
)
select *,(RollingPeople_vaccinated/population)*100
from popvsvac

---------------------------------------------------------------------------------
--USE TEMP TAble
--if you want to make alteration, drop the table and create 

drop table if exists #PercentPopulationVaccinated
create table #PercentPopulationVaccinated
(
continent nvarchar (255),
Location nvarchar (255),
Date datetime,
Population numeric,
new_vaccinations numeric,
RollingPeople_vaccinated numeric
)
insert into #PercentPopulationVaccinated
select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations
,sum(cast(vac.new_vaccinations as bigint)) over(partition by dea.location order by dea.location,dea.date) as RollingPeople_vaccinated
from CovidDeaths$ as dea
JOIN CovidVaccination$ as vac
	on dea.location=vac.location 
	and dea.date=vac.date
where dea.continent is not null
order by 2,3


select *,(RollingPeople_vaccinated/Population)*100
from #PercentPopulationVaccinated

---------------------------------------------------------------------------------
--creating a view for future reference
--dont include order by statement
--will use this table for visualisation later
create view PercentPopulationVaccinated as
select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations
,sum(cast(vac.new_vaccinations as bigint)) over(partition by dea.location order by dea.location,dea.date) as RollingPeople_vaccinated
from CovidDeaths$ as dea
JOIN CovidVaccination$ as vac
	on dea.location=vac.location 
	and dea.date=vac.date
where dea.continent is not null