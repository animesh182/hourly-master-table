raw_query = """
WITH TimestampSeries AS (
    SELECT generate_series(
        TIMESTAMP %s,
        TIMESTAMP %s,
        INTERVAL '1 hour'
    ) AS hour
),
OverallRequiredSalesData AS (
                SELECT
                DATE_TRUNC('hour', "date") as "period",
                gastronomic_day,
                company,
                                restaurant,
                                        SUM(
                                          COALESCE(
                                                CASE
                                                  WHEN "cost" = 'NaN' THEN 0
                                                  ELSE "cost"
                                                END,
                                          0)
                                        ) AS "cost",
                  SUM(CASE WHEN "user_name" = 'nS3 Deliverect' THEN COALESCE("total_gross",0) * 0.25 ELSE 0 END) as "delivery_cost",
                      SUM("total_net") as "total_net"
        FROM public."SalesData"
          group by 1,2,3,4
    ),
	CompanyRestaurantList AS (
    SELECT DISTINCT company, restaurant
    FROM OverallRequiredSalesData
),
TimeStampData as (
SELECT 
    ts.hour as period,
    crl.company,
    crl.restaurant,
	CASE 
		WHEN EXTRACT(HOUR FROM ts.hour AT TIME ZONE 'CEST') < 7 
		THEN (ts.hour AT TIME ZONE 'CEST' - INTERVAL '1 day')::date
		ELSE (ts.hour AT TIME ZONE 'CEST')::date
	END AS gastronomic_day,    
        COALESCE(osd.cost, 0) AS cost,
    COALESCE(osd.delivery_cost, 0) AS delivery_cost,
    COALESCE(osd.total_net, 0) AS total_net
FROM TimestampSeries ts
CROSS JOIN CompanyRestaurantList crl
LEFT JOIN OverallRequiredSalesData osd ON DATE_TRUNC('hour', osd.period) = ts.hour AND osd.company = crl.company AND osd.restaurant = crl.restaurant
),
 OverallRequiredPredictionData as (
        select
    to_timestamp( TO_CHAR("date", 'YYYY-MM-DD') || ' ' || TO_CHAR("hour", 'HH24:MI:SS') , 'YYYY-MM-DD HH24:MI:SS') as date,
        company,
        restaurant,
        "total_gross" as "predicted_sales",
        ROW_NUMBER() OVER (PARTITION BY company, restaurant,date,hour ORDER BY created_at DESC) AS rn
                from public."Predictions_predictionsbyhour"
        ),
TimeStampPredictionData as  (
		SELECT 
    ts.hour as date,
    		CASE 
		WHEN EXTRACT(HOUR FROM ts.hour AT TIME ZONE 'CEST') < 7 
		THEN (ts.hour AT TIME ZONE 'CEST' - INTERVAL '1 day')::date
		ELSE (ts.hour AT TIME ZONE 'CEST')::date
	END AS gastronomic_day,
    crl.company,
    crl.restaurant,
    COALESCE(opd.predicted_sales, 0) AS predicted_sales
FROM TimestampSeries ts
CROSS JOIN CompanyRestaurantList crl
LEFT JOIN OverallRequiredPredictionData opd ON DATE_TRUNC('hour', opd.date) = ts.hour AND opd.company = crl.company AND opd.restaurant = crl.restaurant and opd.rn=1
),
        MonthlyTotalSales as (
        select
                orpd."date" as period,
                orpd.company,
                orpd.restaurant,
                COALESCE(total_net,predicted_sales) as "hourly_sale"
                from TimeStampPredictionData orpd
                left join TimeStampData orsd  on
                        orsd.period=orpd.date
                        and orsd.restaurant=orpd.restaurant
                        and orsd.company=orsd.company
        ),
        MonthlyAggregation as (
                select
            TO_CHAR("period", 'YYYY MM') AS month_year,
                        company,
                                                restaurant,
                        SUM("hourly_sale") as "monthly_sale"
                from MonthlyTotalSales
                group by 1,2,3
                order by 1
        ),
--------------Monthly Total Sales
FilteredSalesData as (
  select
          period,
          company,
			gastronomic_day,
                restaurant,
          SUM("cost") as "cost",
          SUM("delivery_cost") as "delivery_cost",
          SUM("total_net") as "total_net"
          from TimeStampData
          group by 1,2,3,4
  ),
EmployeeCosts AS (
    SELECT
    to_timestamp( TO_CHAR("date", 'YYYY-MM-DD') || ' ' || TO_CHAR("hour", 'HH24:MI:SS') , 'YYYY-MM-DD HH24:MI:SS') as date,
        company,
		restaurant,
        SUM(employee_cost) as employee_hourly_cost
    FROM public."Predictions_hourlyemployeecostandhoursinfo"
    GROUP BY 1,2,3
),
  CompanyNames as (
  select
        id,
         name
          from accounts_company
  ),
  RestaurantCosts as (
	  select 
	  start_date,
	  end_date,
	  restaurant_id,
	  minimum_rent as minimum_rent,
	  rent_variable_sum as rent_variable_sum,
	  fixed_costs as fixed_cost,
	  created_at
      from public."accounts_restaurantcosts" 
	 
  ),
  Restaurants as (
     SELECT
        ar.id AS restaurant_id,
	  	ar.company_id,
        ar.name,
        roh.day_of_week,
        roh.start_hour AS "opening_hour",
        roh.end_hour AS "ending_hour",
        roh.start_date,
        roh.end_date,
        roh.created_at,
	  arc.minimum_rent,
	  arc.rent_variable_sum,
      arc.fixed_cost,
        ts.hour::date AS query_date,
	          CASE 
            WHEN roh.end_hour = roh.start_hour THEN 1
            ELSE CASE
                WHEN roh.end_hour > roh.start_hour THEN roh.end_hour - roh.start_hour
                ELSE roh.end_hour + 24 - roh.start_hour
            END
        END AS opening_duration,
        ROW_NUMBER() OVER (
            PARTITION BY roh.restaurant_id, day_of_week,ts.hour::date
            ORDER BY roh.created_at,arc.created_at DESC
        ) AS rn
    FROM public."accounts_restaurant" ar
    CROSS JOIN TimestampSeries ts 
    LEFT JOIN public."accounts_openinghours" roh ON ar.id = roh.restaurant_id AND ts.hour::date BETWEEN roh.start_date AND roh.end_date AND EXTRACT(DOW FROM ts.hour::date) = roh.day_of_week
      LEFT JOIN RestaurantCosts arc ON ar.id = arc.restaurant_id AND ts.hour::date BETWEEN arc.start_date and arc.end_date
  ),
  Rent as (
  select
		cn.name as "company",
		ar."name" as "restaurant",
		ar."day_of_week",
		ar."opening_hour",
		ar."ending_hour",
		ar."opening_duration",
	  	query_date,
        COALESCE("minimum_rent",0) as "rent",
        COALESCE("rent_variable_sum",0) as "variable_rent",
        COALESCE("fixed_cost",0) as "fixed_cost"
  from CompanyNames cn
  left join Restaurants ar on
        cn.id = ar.company_id
	  and ar.rn=1
  ),
    ActualRent as (
  select
        fsd.*,
CASE
    WHEN r.opening_hour = r.ending_hour THEN
        0
    WHEN r.opening_hour > r.ending_hour AND (EXTRACT(HOUR FROM fsd.period) <= r.ending_hour or EXTRACT(HOUR FROM fsd.period)>=opening_hour) THEN
        CASE
            WHEN r."rent" > COALESCE(ma."monthly_sale") * COALESCE(r."variable_rent") / 100 THEN
                COALESCE(r."rent", 0) / (30.5 * r.opening_duration)
            ELSE
                COALESCE(ma."monthly_sale") * COALESCE(r."variable_rent") / (100 * 30.5 * r.opening_duration)
        END
    WHEN EXTRACT(HOUR FROM fsd.period) >= r.opening_hour AND EXTRACT(HOUR FROM fsd.period) <= r.ending_hour THEN
        CASE
            WHEN r."rent" > COALESCE(ma."monthly_sale") * COALESCE(r."variable_rent") / 100 THEN
                COALESCE(r."rent", 0) / (30.5 * r.opening_duration)
            ELSE
                COALESCE(ma."monthly_sale") * COALESCE(r."variable_rent") / (100 * 30.5 * r.opening_duration)
        END
    ELSE
        0
END AS "rent",
CASE
    WHEN r.opening_hour = r.ending_hour THEN
        0
    WHEN r.opening_hour > r.ending_hour AND (EXTRACT(HOUR FROM fsd.period) <= r.ending_hour or EXTRACT(HOUR FROM fsd.period)>=opening_hour) THEN
        r.fixed_cost/(30.5 * r.opening_duration)
    WHEN EXTRACT(HOUR FROM fsd.period) >= r.opening_hour AND EXTRACT(HOUR FROM fsd.period) <= r.ending_hour THEN
        r.fixed_cost/(30.5 * r.opening_duration)
    ELSE
        0
END AS "fixed_cost",
"employee_hourly_cost" as employee_cost
          from FilteredSalesData fsd
          left join rent r on fsd."company"=r."company" and fsd."restaurant"=r."restaurant" and fsd.gastronomic_day = r.query_date
          left join  MonthlyAggregation ma on TO_CHAR(fsd."period", 'YYYY MM') = ma."month_year" 
                and fsd."company"=ma."company" and ma."restaurant"=fsd."restaurant"
            left join EmployeeCosts ec on
                ec."company"=fsd."company"
          and ec."date" = fsd."period"
		and ec."restaurant" =fsd."restaurant"


	),  
  TempData as (
    select 
        "period",
	  	gastronomic_day,
        "company",
	  	"restaurant",
        SUM(cost) as "cost",
        SUM(delivery_cost) as "delivery_cost",
        sum(total_net) as "total_net",
        sum(rent) as "rent",
	  	sum(employee_cost) as "employee_cost",
        sum(fixed_cost) as "fixed_cost"
    from ActualRent
    group by 1,2,3,4
  ),
  HistoricalData as (
        select
          ar.*,
          ar."total_net" - COALESCE(ar."cost",0) as "gross_profit",
          ar."total_net"- COALESCE(ar."cost",0)- COALESCE(ar."delivery_cost",0) - COALESCE(ar."rent",0) - COALESCE(ar."employee_cost",0) - fixed_cost as "net_profit",
          case when
          COALESCE(ar."total_net",0) = 0 then 0
          else (ar."total_net" - COALESCE(ar."cost",0))*100/ar."total_net"
          end as "gross_profit_percentage",
          case when COALESCE(ar."total_net",0)=0 then 0
          else (ar."total_net"- COALESCE(ar."cost",0)- COALESCE(ar."delivery_cost",0) - COALESCE(ar."rent",0) - COALESCE(ar."employee_cost",0) - fixed_cost)*100/ar."total_net" 
          end as "net_profit_percentage"
          from TempData ar
  )
 insert into public."HistoricalMasterTable"(	id, date, gastronomic_day, total_net, cost, gross_profit, delivery_cost, rent, employee_cost, fixed_cost, net_profit, gross_profit_percentage, net_profit_percentage, company, restaurant)
select 
	FLOOR(RANDOM() * 9223372036854775807::bigint) + 1::BIGINT,
	hv.period,
	hv.gastronomic_day,
	COALESCE(hv.total_net,0),
	COALESCE(hv.cost,0),
	COALESCE(gross_profit,0),
	COALESCE(delivery_cost,0),
	COALESCE(rent,0),
	COALESCE(employee_cost,0),
	COALESCE(fixed_cost,0),
	COALESCE(net_profit,0),
	COALESCE(gross_profit_percentage,0),
	COALESCE(net_profit_percentage,0),
	hv.company,
	hv.restaurant
	from HistoricalData hv
    ON CONFLICT (date,restaurant,company)
    DO UPDATE SET
        total_net = EXCLUDED.total_net,
        cost = EXCLUDED.cost,
        gross_profit = EXCLUDED.gross_profit,
        delivery_cost = EXCLUDED.delivery_cost,
        rent = EXCLUDED.rent,
        employee_cost = EXCLUDED.employee_cost,
        fixed_cost = EXCLUDED.fixed_cost,
        net_profit = EXCLUDED.net_profit,
        gross_profit_percentage = EXCLUDED.gross_profit_percentage,
        net_profit_percentage = EXCLUDED.net_profit_percentage
"""
