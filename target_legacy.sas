/*---------------------------------------------------------------------------
  Version: 1.0	  
  Desarrollador: Joseph Chombo					
  Fecha Release: 31/01/2025
-----------------------------------------------------------------------------*/

%macro __plot_describe_Target(data=, targetlocal=, byvarlocal=, timedefault=0);
    *Calcula la RD para el dataset ingresado;
    *Calcula la materialidad;
 
    Data temp_data;
        set &data.;
        %if &timedefault. ^= 0 %then %do; 
            %PUT default cerrado hasta &timedefault;
            where &byvarlocal. <= &timedefault.;
        %end;
    run;

    proc means data=temp_data n mean; 
        var &targetlocal.; 
        class &byvarlocal.; 
        output out=temp_evolut_target n=N mean=Mean;
    run;
 
    data temp_evolut_target2;
        set temp_evolut_target;
        where _TYPE_ ne 0;
    run;
	    * Contar el número de meses;
    proc sql noprint;
        select count(distinct &byvarlocal.) into :num_months
        from temp_evolut_target2;
    quit;
	%if &num_months ne 1 %then %do;
		%if &num_months >= 6 %then %do;
			 	* Seleccionar los primeros y últimos tres meses;
			    data first_months last_months;
			        set temp_evolut_target2 nobs=nobs;
			        if _N_ <= 3 then output first_months;
			        if _N_ > (nobs - 3) then output last_months;
			    run;
			    * Calcular los promedios de los primeros y últimos tres meses;
			    proc means data=first_months mean noprint;
			        var Mean;
			        output out=first_mean mean=first_mean;
			    run;
			    proc means data=last_months mean noprint;
			        var Mean;
			        output out=last_mean mean=last_mean;
			    run;
			    data _null_;
			        set first_mean;
			        call symputx('first_mean', first_mean);
			    run;
			    data _null_;
			        set last_mean;
			        call symputx('last_mean', last_mean);
			    run;
			    %let relative_diff = %sysevalf((&last_mean - &first_mean) / &first_mean);

			    * Mostrar los resultados en una tabla;
			       * Crear una tabla con los resultados;
			    data results;
			        length Metric $30 Value 8.;
			        Metric = "Promedio de los primeros 3 meses"; Value = &first_mean; output;
			        Metric = "Promedio de los últimos 3 meses"; Value = &last_mean; output;
			        Metric = "Diferencia relativa"; Value = &relative_diff; output;
			    run;
			    title "Resultados de la Diferencia Relativa";
			    proc print data=results noobs;
			        var Metric Value;
			    run;
                title;
		%end;
		%else %do;
					 	* Seleccionar los primeros y últimos tres meses;
			    data first_months last_months;
			        set temp_evolut_target2 nobs=nobs;
			        if _N_ <= 1 then output first_months;
			        if _N_ > (nobs - 1) then output last_months;
			    run;
			    * Calcular los promedios de los primeros y últimos tres meses;
			    proc means data=first_months mean noprint;
			        var Mean;
			        output out=first_mean mean=first_mean;
			    run;
			    proc means data=last_months mean noprint;
			        var Mean;
			        output out=last_mean mean=last_mean;
			    run;
			    data _null_;
			        set first_mean;
			        call symputx('first_mean', first_mean);
			    run;
			    data _null_;
			        set last_mean;
			        call symputx('last_mean', last_mean);
			    run;
			    %let relative_diff = %sysevalf((&last_mean - &first_mean) / &first_mean);

			    * Mostrar los resultados en una tabla;
			       * Crear una tabla con los resultados;
			    data results;
			        length Metric $30 Value 8.;
			        Metric = "Primer mes"; Value = &first_mean; output;
			        Metric = "Ultimo mes"; Value = &last_mean; output;
			        Metric = "Diferencia relativa"; Value = &relative_diff; output;
			    run;
			    title "Resultados de la Diferencia Relativa";
			    proc print data=results noobs;
			        var Metric Value;
			    run;
                title;
		%end;
	%end;
 
    title Evolutivo &targetlocal.;
    proc sgplot data=temp_evolut_target2;
        vline &byvarlocal / response=Mean markers markerattrs=(symbol=circlefilled COLOR=black) lineattrs=(color=crimson);
        yaxis label="mean &targetlocal" min=0 max=1;    
    run;
    title;

    title Materialidad &data. Cerrado;
    proc freq data=temp_data;
        tables &byvarlocal. * &targetlocal. /norow nopercent nocum nocol;
    run;
    title;
%mend;

%macro __plot_bandas_target(macro_table=, data_type=, macro_time=, macro_target=, existe_train=, clean_globals=); 
    %local temp_global_avg temp_std inf sup min_val max_val;
    %global global_avg std_monthly;
    

    proc sql; 
        create table monthly as 
        select &macro_time, 
            mean(&macro_target) as avg_target 
        from &macro_table 
        group by &macro_time 
        order by &macro_time; 
    quit;
 
    %if (&existe_train=0) or (%length(&global_avg)=0) %then %do;
        proc sql noprint; 
            select mean(&macro_target) into :temp_global_avg separated by ' ' from &macro_table; 
        quit; 
        %let global_avg = &temp_global_avg;
 
        proc sql noprint; 
            select std(avg_target) into :temp_std separated by ' ' from monthly; 
        quit; 
        %let std_monthly = &temp_std;
    %end;
    %else %do;
        %put NOTE: Usando estadistica existentes (global_avg=&global_avg, std_monthly=&std_monthly);
    %end;
 
    %let inf = %sysevalf(&global_avg - 2 * &std_monthly); 
    %let sup = %sysevalf(&global_avg + 2 * &std_monthly); 
    %let min_val = %sysevalf(&global_avg - 3 * &std_monthly); 
    %let max_val = %sysevalf(&global_avg + 3 * &std_monthly); 
    %put inf: &inf - sup: &sup - global_avg: &global_avg - std: &std_monthly;
    
    data monthly_with_bands;
        set monthly;
        lower_band = &inf;
        upper_band = &sup;
        global_avg = &global_avg;
        format avg_target lower_band upper_band global_avg 8.4;
    run;
    title "Evolutivo del Target - &data_type";
    proc sgplot data=monthly subpixel noautolegend;
        band x=&macro_time lower=&inf upper=&sup / fillattrs=(color=graydd) legendlabel="± 2 Desv. Estandar" name="band1";
        series x=&macro_time y=avg_target / markers lineattrs=(color=blue thickness=2) legendlabel="RD" name="serie1";
        refline &global_avg / lineattrs=(color=red pattern=Dash) legendlabel="Overall Mean" name="line1"; 
        yaxis min=&min_val max=&max_val label="Promedio de &macro_target";
        xaxis label="&macro_time" type=discrete;
        keylegend "serie1" "band1" / location=inside position=bottomright;
    run;
    title;
    proc print data=monthly_with_bands noobs;
        var &macro_time avg_target lower_band upper_band global_avg;
        label 
            &macro_time = "Periodo"
            avg_target = "Promedio del Target" 
            lower_band = "Límite Inferior (- 2 Desv.)"
            upper_band = "Límite Superior (+ 2 Desv.)"
            global_avg = "Promedio Global";
    run;
    proc datasets library=work nolist;
        delete monthly monthly_with_bands;
    quit;
    
    %if &clean_globals=1 %then %do;
        %symdel global_avg std_monthly;
    %end;
%mend;

%macro __target_ponderado_promedio(data=, data_type=, target=, monto=, byvar=, existe_train=, clean_globals=);
    %local temp_global_avg temp_std inf sup min_val max_val;
    %global global_avg_pond std_monthly_pond;
    
    /* Calcular promedio ponderado por mes */
    proc sql; 
        create table monthly_pond as 
        select &byvar, 
               /* Promedio ponderado: sum(target*monto)/sum(monto) */
               sum(&target * &monto) / sum(&monto) as avg_target_pond
        from &data 
        where &monto > 0  /* Evitar divisiones por cero */
        group by &byvar
        order by &byvar; 
    quit;
    
    /* Calcular estadísticas globales si es TRAIN o no existen */
    %if (&existe_train=0) or (%length(&global_avg_pond)=0) %then %do;
        proc sql noprint; 
            /* Promedio global ponderado */
            select sum(&target * &monto) / sum(&monto) into :temp_global_avg 
            from &data
            where &monto > 0; 
        quit; 
        %let global_avg_pond = &temp_global_avg;
        
        proc sql noprint; 
            select std(avg_target_pond) into :temp_std 
            from monthly_pond; 
        quit; 
        %let std_monthly_pond = &temp_std;
    %end;
    %else %do;
        %put NOTE: Usando estadisticas existentes (global_avg_pond=&global_avg_pond, std_monthly_pond=&std_monthly_pond);
    %end;
    
    /* Calcular límites para las bandas */
    %let inf = %sysevalf(&global_avg_pond - 2 * &std_monthly_pond); 
    %let sup = %sysevalf(&global_avg_pond + 2 * &std_monthly_pond); 
    %let min_val = %sysevalf(&global_avg_pond - 5 * &std_monthly_pond); 
    %let max_val = %sysevalf(&global_avg_pond + 5 * &std_monthly_pond); 
    %put inf: &inf - sup: &sup - global_avg_pond: &global_avg_pond - std_pond: &std_monthly_pond;
    
    data monthly_pond_with_bands;
        set monthly_pond;
        lower_band = &inf;
        upper_band = &sup;
        global_mean = &global_avg_pond;
        format avg_target_pond lower_band upper_band global_mean 8.6;
    run;
    title "Target Ponderado por Monto - &data_type";
    proc sgplot data=monthly_pond subpixel noautolegend;
        band x=&byvar lower=&inf upper=&sup / fillattrs=(color=graydd) 
            legendlabel="± 2 Desv. Estandar" name="band1";
        series x=&byvar y=avg_target_pond / markers lineattrs=(color=darkblue thickness=2) 
            legendlabel="RD Pond. Promedio" name="serie1";
        refline &global_avg_pond / lineattrs=(color=red pattern=Dash) 
            legendlabel="Media Ponderada Global" name="line1"; 
        yaxis min=&min_val max=&max_val label="RD Pond. por Monto";
        xaxis label="&byvar" type=discrete;
        keylegend "serie1" "band1" "line1" / location=inside position=bottomright;
        
    run;
    title;

…