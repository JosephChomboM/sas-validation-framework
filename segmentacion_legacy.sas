/*---------------------------------------------------------------------------
  Version: 1.0
  Test de Kolmogorov-Smirnov para heterogeneidad entre duplas de segmentos basado en el target
  Desarrollador: Joseph Chombo					
  Fecha: 23/03/2025
-----------------------------------------------------------------------------*/
%macro __kolmogorov_smirnov(data=, segmentadora=, target=, data_type=, output_table=ks_resultados);

    proc format;
        value 
statusks 'DIFERENTES'    = 'LightGreen'
                        'SIMILARES' = 'LightRed';
    run;
    
    data &output_table;
        length Segmento1 8 Segmento2 8 
               KS_Statistic 8 KS_Asymptotic 8 D_Statistic 8 
               P_Value 8 Prueba_KS $20;
        stop;
    run;
    
    /* Obtener lista de segmentos únicos */
    proc sql noprint;
        select distinct &segmentadora into :segmentos separated by ' '
        from &data;
        
        select count(distinct &segmentadora) into :num_segmentos
        from &data;
    quit;
    
    %put NOTE: Se encontraron &num_segmentos segmentos: &segmentos;

    /* Procesar cada combinación de pares de segmentos */
    %local i j seg1 seg2;
    %do i = 1 %to %eval(&num_segmentos - 1);
        %let seg1 = %scan(&segmentos, &i);
        
        %do j = %eval(&i + 1) %to &num_segmentos;
            %let seg2 = %scan(&segmentos, &j);
            
            %put NOTE: Procesando combinacion &seg1 con &seg2;
            
            /* Crear dataset con solo los dos segmentos a comparar */
            data dos_segmentos;
                set &data;
                where &segmentadora in (&seg1, &seg2);
                keep &segmentadora &target;
            run;
            
            /* Contar registros para asegurar que hay suficientes datos */
            proc sql noprint;
                select count(*) into :n_obs from dos_segmentos;
            quit;
            
            %if &n_obs > 10 %then %do; /* Asegurar un mínimo de observaciones */
                /* Ejecutar test KS */
                ods select none;
                proc npar1way data=dos_segmentos KS;
                    class &segmentadora;
                    var &target;
                    output out=ks_temp;
                run;
                ods select all;
                
                /* Verificar si se generó la salida */
                %let dsid = %sysfunc(open(ks_temp));
                %if &dsid > 0 %then %do;
                    %let rc = %sysfunc(close(&dsid));
                    
                    /* Extraer y formatear los resultados */
                    data ks_result;
                        length Segmento1 8 Segmento2 8 
                               KS_Statistic 8 KS_Asymptotic 8 D_Statistic 8 
                               P_Value 8 Prueba_KS $20;
                        set ks_temp;
                        
                        /* Solo necesitamos la primera fila que contiene los resultados del KS test */
                        if _N_ = 1;
                        
                        /* Asignar valores de segmentos */
                        Segmento1 = &seg1;
                        Segmento2 = &seg2;
                        
                        /* Extraer estadísticos relevantes */
                        KS_Statistic = _KS_;
                        KS_Asymptotic = _KSA_;
                        D_Statistic = _D_;
                        P_Value = P_KSA;
                        
                        /* Evaluar significancia estadística */
                        if P_Value < 0.05 then 
                            Prueba_KS = "DIFERENTES";
                        else 
                            Prueba_KS = "SIMILARES";
                        
                        /* Mantener solo las variables necesarias */
                        keep Segmento1 Segmento2 KS_Statistic KS_Asymptotic 
                             D_Statistic P_Value Prueba_KS;
                    run;
                    
                    /* Anexar al dataset final */
                    proc append base=&output_table data=ks_result; 
                    run;
                %end;
                %else %do;
                    %put WARNING: No se generaron resultados para la combinación &seg1 vs &seg2;
                %end;
            %end;
            %else %do;
                %put WARNING: Insuficientes observaciones para la combinación &seg1 vs &seg2 (n=&n_obs);
            %end;
        %end;
    %end;
    
    title "Test de Heterogeneidad Kolmogorov-Smirnov entre Segmentos - &data_type";
    proc print data=&output_table noobs;
        var Segmento1 Segmento2 KS_Statistic D_Statistic P_Value;
        var Prueba_KS/style={background=
statusks.};
        format P_Value pvalue6.4 KS_Statistic D_Statistic 6.4;
    run;
    title;
    /* Resumen de heterogeneidad */
    proc sql;
        create table resumen_&output_table as
        select count(*) as Total_Pares,
               sum(case when Prueba_KS = 'DIFERENTES' then 1 else 0 end) as Pares_Diferentes,
               calculated Pares_Diferentes / calculated Total_Pares as Proporcion_Diferentes format=percent8.1
        from &output_table;
    quit;
        proc print data=resumen_&output_table noobs;
    run;
    
    proc datasets library=work nolist;
        delete dos_segmentos ks_temp ks_result ks_resultados;
    quit;
    

%mend;
%macro __kruskall(table,seg=,targ=,byvarl=,where=.);
	%let totalCuentas=0;
    ods select none;
    proc means data=&table mean;
        class &seg &byvarl;
        var &targ;
        ods output summary=means;
        %if &where ne . %then %do; where &where; %end;
    run;

	proc sql noprint;
		select sum(NObs) into :totalCuentas
		from means;
	quit;

	proc sql noprint;
		create table means2 as select *,
			 (NObs/&totalCuentas)*100 as Porcentaje 
		from means;
	quit;
	
    %if &seg. ne %then %do;
        proc npar1way data=means2;
            class &seg;
            var &targ._Mean;
            Ods output KruskalWallisTest=report;
        run;
    %end;
    ods select all;
    %if &seg. ne %then %do;
    proc print data=report noobs; run;
    proc sql; drop table report; quit;

    %end;
    proc print data=means2 noobs;

   proc sql; drop table means, means2; quit;

%mend;
/*---------------------------------------------------------------------------
  Version: 1.0
  Análisis de Materialidad de Segmentos
  Desarrollador: Joseph Chombo					
  Fecha: 15/03/2025
-----------------------------------------------------------------------------*/
/*--------------------------------------------------------------
|   METOD 3: Se tiene materialidad de los segmentos
        (mínimo 1000 observaciones por segmento) en caso la cartera lo permita
    Se tiene materialidad del target por segmento (mínimo 450 observaciones),
    en caso la cartera lo permita
----------------------------------------------------------------*/

%macro __segmentacion_materialidad(m_data=, m_target=, m_segmentadora=,  m_data_type=, m_min_obs=1000, m_min_target=450);
    
    proc format;
    value 
statusmtd 'CUMPLE'    = 'LightGreen'
                    'NO CUMPLE' = 'LightRed';
    run;
    proc sql noprint;
        /* Contar registros totales */
        select count(*) into :total_obs from &m_data;
        /* Contar targets totales */
        select sum(&m_target) into :total_target from &m_data;
    quit;
    
    data work.global_materialidad;
        length Tipo_Muestra $20 Materialidad 8 Cantidad_Target 8 Verif_Materialidad $10 Verif_Target $10;
        Tipo_Muestra = "&m_data_type";
        Materialidad = &total_obs;
        Cantidad_Target = &total_target;
        if Materialidad >= &m_min_obs then Verif_Materialidad = 'CUMPLE';
        else Verif_Materialidad = 'NO CUMPLE';
        if Cantidad_Target >= &m_min_target then Verif_Target = 'CUMPLE';
        else Verif_Target = 'NO CUMPLE';
    run;
    
    data work.r&m_data_type;
        cumple_materialidad = (Materialidad >= &m_min_obs);
        cumple_target = (Cantidad_Target >= &m_min_target);
        cumple_ambos = (cumple_materialidad and cumple_target);
        set work.global_materialidad;
        format cumple_materialidad cumple_target cumple_ambos 1.;
    run;
    
    
    title "Validacion de Suficiencia Global - &m_data_type";
    title2 "Minimo &m_min_obs observaciones y &m_min_target defaults";
    
    proc print data=work.global_materialidad
        style(column)={backgroundcolor= statusmtd.} noobs label;
        var Tipo_Muestra Materialidad;
        var Verif_Materialidad /style={background=
statusmtd.};
        var Cantidad_Target;
        var Verif_Target /style={background=
statusmtd.};
        label Cantidad_Target = 'Total Default';
    run;
    title;
    title2;

    %if &exist_segm = 1 %then %do;
        proc format;
        value 
statusmtd 'CUMPLE'    = 'LightGreen'
                        'NO CUMPLE' = 'LightRed';
        run;
        proc sql noprint;
            /* Contar registros por segmento */
            create table work.seg_count as
            select 
                &m_segmentadora as Segmento, 
                count(*) as Materialidad
            from &m_data
            group by &m_segmentadora order by &m_segmentadora;
            
            /* Contar defaults por segmento */
            create table work.seg_target as
            select 
                &m_segmentadora as Segmento, 
                sum(&m_target) as Cantidad_Target
            from &m_data
            group by &m_segmentadora order by &m_segmentadora;
            
            /* Juntar ambas tablas y calcular flags */
            create table work.seg_materialidad as
            select 
                a.Segmento, 
                a.Materialidad, 
                b.Cantidad_Target,
                case when a.Materialidad >= &m_min_obs then 'CUMPLE' else 'NO CUMPLE' end as Verif_Materialidad,
                case when b.Cantidad_Target >= &m_min_target then 'CUMPLE' else 'NO CUMPLE' end as Verif_Target
            from work.seg_count a
            join work.seg_target b on a.Segmento = b.Segmento order by a.Segmento;
            
            select count(distinct Segmento) into :total_segmentos from work.seg_materialidad;
            select count(*) into :cumplen_materialidad
            from work.seg_materialidad where Verif_Materialidad = 'CUMPLE';
            select count(*) into :cumplen_target
            from work.seg_materialidad where Verif_Target = 'CUMPLE';
            select count(*) into :cumplen_ambos
            from work.seg_materialidad where Verif_Materialidad = 'CUMPLE' and Verif_Target = 'CUMPLE';
        quit;
        
        data work.r&m_data_type;
            total_segmentos = &total_segmentos;
            cumplen_materialidad = &cumplen_materialidad;
            cumplen_target = &cumplen_target;
            cumplen_ambos = &cumplen_ambos;
            PCT_cumplimiento = cumplen_ambos / total_segmentos;
            format PCT_cumplimiento percent8.2;
        run;
        title "Materialidad de Segmentos - &m_data_type";
        title2 "Minimo &m_min_obs obs y &m_min_target defaults por segmento";
        
        proc print data=work.seg_materialidad
            style(column)={backgroundcolor= statusmtd.} noobs label;
            var Segmento Materialidad;
            var Verif_Materialidad /style={background=
statusmtd.};
            var  Cantidad_Target;
            var Verif_Target /style={background=
statusmtd.};
            label Cantidad_Target = 'Total Default';
        run;
        title;
        title2;
        
        proc print data=work.r&m_data_type noobs label;
        run;
        /* Limpiar tablas temporales */
        proc datasets lib=work nolist;
            delete seg_count seg_target seg_materialidad;
        quit;
    %end;
    proc datasets lib=work nolist;
        delete global_materialidad r&m_data_type;
    run;
    
%mend __segmentacion_materialidad;
/*---------------------------------------------------------------------
    Version: 1.0 
    Análisis de Migracion de Segmentos
    Fecha: 31/01/2025                              
-----------------------------------------------------------------------*/
/*--------------------------------------------------------------
|   METOD 3: Se analiza la migracion de segmentos de las
    cuentas / clientes bajo distintas ventanas temporales
----------------------------------------------------------------*/
options spool;
%macro __migra_segmentos(m_data=, m_idDataset=, m_segmentadora=, m_time=, primer_mes=, ultimo_mes=, m_data_type=);
    %PUT SE IMPRIME &m_data - id: &m_idDataset - varseg: &m_segmentadora - tiempo: &m_time - primer: &primer_mes - ultimo: &ultimo_mes - tipo: &m_data_type;
	/* Data para primer y ultimo mes */
	data primer_mes;
        set &m_data;
        where &m_time = &primer_mes;
        rename &m_segmentadora = seg_primer_mes;
        keep &m_idDataset &m_segmentadora;
    run;

	proc sort data=primer_mes; by &m_idDataset; run;

    data ultimo_mes;
        set &m_data;
        where &m_time = &ultimo_mes;
        rename &m_segmentadora = seg_ultimo_mes;
        keep &m_idDataset &m_segmentadora;
    
    run;
	
	proc sort data=ultimo_mes; by &m_idDataset; run;

	/* Unir las datas para migracion */
    data merge_mes;
        merge primer_mes(in=a) ultimo_mes(in=b);
        by &m_idDataset;
        length tipo_cliente $10;
        if a and b then tipo_cliente = 'CRUCE';
        else if a then tipo_cliente = 'RETIRADO';
        else if b then tipo_cliente = 'NUEVO';
    run;
	/* Data de retirados y nuevos*/
    ods select none;
	proc freq data=merge_mes;
        where tipo_cliente = 'RETIRADO';
        tables seg_primer_mes / nocum nocol;
        ods output OneWayFreqs=retirados;
    run;
    data retirados;
        set retirados;
        rename seg_primer_mes=Segmento Frequency=Cant_Retirados Percent=Pct_Retirados;
    run;
    proc freq data=merge_mes ;
        where tipo_cliente = 'NUEVO';
        tables seg_ultimo_mes / nocum nocol;
        ods output OneWayFreqs=nuevos;
    run;
    data nuevos;
        set nuevos;
        rename seg_ultimo_mes=Segmento Frequency=Cant_Nuevos Percent=Pct_Nuevos;
    run;
	ods select all;

    data migracion;
        merge retirados(in=a) nuevos(in=b);
        by Segmento;
        format Pct_Retirados Pct_Nuevos;
    run;

    title "Migracion de Segmentos - &m_data_type";
    proc print data=migracion noobs;
        var Segmento Cant_Retirados Pct_Retirados Cant_Nuevos Pct_Nuevos;
        format Pct_Retirados Pct_Nuevos;
    run;

    title "Matriz de Migracion entre Segmentos - &m_data_type";

    ods listing style=minimal;

    /* Crear tabla de doble entrada solo con los valores y porcentajes*/
    ods noproctitle;
    proc freq data=merge_mes;
        where tipo_cliente = 'CRUCE';
        tables seg_primer_mes*seg_ultimo_mes / nocol nocum nopercent out=cruce_raw;
        ods output CrossTabFreqs=cruce;
    run;

    title2;

    title "Distribucion por Tipo de Cliente - &m_data_type";
	ods select none;
    proc freq data=merge_mes;
        tables tipo_cliente / nocum;
        ods output OneWayFreqs=proporciones;
    run;
    ods select all;

    data resumen;
        length Indicador $40;
        set proporciones(rename=(tipo_cliente=Indicador Frequency=Frequency Percent=PCT_total));
        keep Indicador Frequency PCT_total;
    run;

    /* Mostrar resultados */
    title "Migracion de Clientes - &m_data_type";
    title2 "&primer_mes - &ultimo_mes";
    proc print data=resumen noobs; run;
    title;
    title2;
    proc template;
        define statgraph migrationplot;
            begingraph;
                entrytitle "Migracion entre Segmentos - &m_data_type";
                layout overlay / xaxisopts=(label="Segmento Inicial") 
                                yaxisopts=(label="Segmento Final");
                    heatmapparm x=seg_primer_mes y=seg_ultimo_mes colorresponse=Percent / 
                        name="heatmap" colormodel=ThreeColorRamp
                        primary=true display=all;
                    continuouslegend "heatmap" / title="Porcentaje";
                endlayout;
            endgraph;
        end;
    run;
    
    proc sgrender data=cruce_raw template=migrationplot;
    run;
    
    proc datasets lib=work nolist;
        delete primer_mes ultimo_mes merge_mes retirados nuevos cruce_raw proporciones resumen migracion;
    quit;
%mend;
%macro __plot_segmentation(base,target=,byvarl=,segmentador=, sep=0);
	%local xticks;
	%local xticks_;
	
    %_get_mod_list_by_var(tabla = &base, byvar = &byvarl, byvarvalues = xticks, 
						byvar_mod = xticks_, mod = 1, nmatch = 2);

	%if &sep = 0 %then %do;
	    title Distribucion mensual &segmentador;
	    proc sgplot data=&base;
	    vbar &byvarl/ NOOUTLINE GROUP=&segmentador;
	    vline &byvarl /response=&target GROUP=&segmentador  markers STAT=MEAN markerattrs=(symbol=circlefilled) Y2AXIS;
	    YAXIS LABEL="Cuentas";
	    y2axis min=0 label="mean &target" valuesformat=percentn8.0;
		xaxis values=(&xticks) valuesdisplay=(&xticks_); 
	    run;
	    title ;

		/*proc means data=&base n mean; var &target; class &byvar &var; run;*/
	%end;

	%else %do;
		proc sql;
    		create table cuenta_dist_temp as
    		select &segmentador, &byvarl,
			count(*) as cuentas,
			(select count(*) from &base where &byvarl = a.&byvarl) as total_por_&byvarl
            from &base as a
	        group by &byvarl, &segmentador;
        quit;

       proc sql;
	       create table cuenta_dist as
	       select *,
	       (cuentas / total_por_&byvarl)*100 as pct
	      from cuenta_dist_temp;
       quit;

	    title Distribucion de cuentas &Segmentador;
	    proc sgplot data=&base;
	    vbar &byvarl/ NOOUTLINE GROUP=&Segmentador;
	    YAXIS LABEL="Cuentas";
		xaxis values=(&xticks) valuesdisplay=(&xticks_); 
	    run;
	    title ;

	    title Distribucion de mean &target por segmento &Segmentador;
	    proc sgplot data=&base;
	    vline &byvarl /response=&target GROUP=&Segmentador  markers STAT=MEAN markerattrs=(symbol=circlefilled);
	    yaxis min=0 label="mean &target" valuesformat=percentn8.0;
		xaxis type=discrete values=(&xticks) valuesdisplay=(&xticks_); 
	    run;
	    title ;
	
		proc sgplot data=cuenta_dist;
 		vbar &byvarl / response=cuentas group=&Segmentador groupdisplay=cluster datalabel=pct;
		xaxis display=(nolabel);
 		yaxis label="Cuentas";
		run;

/*	    proc means data=&base n mean; var &target; class &byvar &var; run;*/
	%end;
%mend;

%macro _get_mod_list_by_var(tabla, byvar, byvarvalues = xticks, byvar_mod = xticks_, mod=1, nmatch=2);

	proc sql noprint;
        select distinct &byvar into :&byvarvalues separated by " " from &tabla;
    quit;

	%let freq = 1;
	%local n v t;
	%let n=1;
	%let v=%scan(&&&byvarvalues, &n," ");

	%do %while(%length(&v)^=0); 
		%if %sysfunc(mod(%sysfunc(substr(&v, %length(&v)-&nmatch+1, &nmatch)), &freq)) ne 0 %then %do;
			%let v_ = %str(" ");
			%if &n =1 %then %do;
				%let t = %sysfunc(catx(%str( ), &v_));
			%end;
			%else %do;
				%let t = %sysfunc(catx(%str( ), &t, &v_));
			%end;
		%end;
		%else %do;
			%if &n =1 %then %do;
				%let t = %sysfunc(catx(%str( ), "&v"));
			%end;
			%else %do;
				%let t = %sysfunc(catx(%str( ), &t, "&v"));
			%end;
		%end;
		%let n=%eval(&n+1);
		%let v=%scan(&&&byvarvalues, &n, " ");
	%end;
	%let &byvar_mod = &t;
%mend _get_mod_list_by_var;
%include "&_root_path/Sources/Modulos/m_segmentacion/segmentacion_macro.sas";
%include "&_root_path/Sources/Modulos/m_segmentacion/kruskall_macro.sas";
%include "&_root_path/Sources/Modulos/m_segmentacion/materialidad_macro.sas";
%include "&_root_path/Sources/Modulos/m_segmentacion/migracion_macro.sas";
%include "&_root_path/Sources/Modulos/m_segmentacion/kolmogorov_macro.sas";
%macro __segmentation_report(dataset=, data_type=);
    
    ods graphics on / outputfmt=svg;
    %if &data_type = TRAIN %then %do;
        ods html5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Segmentation_1.html";
        ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Segmentacion.xlsx";
        ods excel options(sheet_name="TRAIN_Segmentation" 
                        sheet_interval="none" 
                        embedded_titles="yes");
        %if &exist_segm ne 0 %then %do;
            %__plot_segmentation(&dataset, target=&_target, byvarl=&_var_time, segmentador=&var_segmentadora);
            %__migra_segmentos(m_data=&dataset, m_idDataset=&_idDataset, m_segmentadora=&var_segmentadora, m_time=&_var_time, primer_mes=&_primer_mes_train, ultimo_mes=&_ultimo_mes_train, m_data_type=&data_type);
            %__kolmogorov_smirnov(data=&dataset, segmentadora=&var_segmentadora, target=&_target, data_type=&data_type, output_table=ks_resultados);
        %end;
        %__kruskall(&dataset, seg=&var_segmentadora, targ=&_target, byvarl=&_var_time);
		%__segmentacion_materialidad(m_data=&dataset, m_target=&_target, m_segmentadora=&var_segmentadora,  m_data_type=&data_type);
    %end;
    %else %if &data_type = OOT %then %do;
        /* Agregar hoja OOT al mismo archivo */
        ods html5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Segmentation_2.html";
        ods excel options(sheet_name="OOT_Segmentation" 
                        sheet_interval="now" 
                        embedded_titles="yes");
        
        %if &exist_segm ne 0 %then %do;
            %__plot_segmentation(&dataset, target=&_target, byvarl=&_var_time, segmentador=&var_segmentadora);
            %__migra_segmentos(m_data=&dataset, m_idDataset=&_idDataset, m_segmentadora=&var_segmentadora, m_time=&_var_time, primer_mes=&_primer_mes_oot, ultimo_mes=&_ultimo_mes_oot, m_data_type=&data_type);
            %__kolmogorov_smirnov(data=&dataset, segmentadora=&var_segmentadora, target=&_target, data_type=&data_type, output_table=ks_resultados);
        %end;
        %__kruskall(&dataset, seg=&var_segmentadora, targ=&_target, byvarl=&_var_time);
		%__segmentacion_materialidad(m_data=&dataset, m_target=&_target, m_segmentadora=&var_segmentadora,  m_data_type=&data_type);
        ods excel close;
        ods html5 close;
    %end;
    ods graphics off;
%mend;
%include "&_root_path/Sources/Modulos/m_segmentacion/segmentacion_report.sas";

%macro verify_segmentation(dataset, data_type=);
    /* Verificación de dataset y variables obligatorias */
    %if %sysfunc(exist(&dataset)) %then %do;
        %let nobs = %sysfunc(attrn(%sysfunc(open(&dataset)),nobs));
        %if &nobs > 0 %then %do;
            %if %length(&_target.) > 0 and %length(&_var_time.) > 0 %then %do;
                
                %let exist_segm = 0;

				%if %length(&var_segmentadora) > 0 %then %do;
					%let exist_segm = 1;
				%end;

                %put NOTE: (Segmentation) Todos los parametros necesarios estan disponibles;
                %__segmentation_report(dataset=&dataset, data_type=&data_type);
            %end;
            %else %do;
                %put WARNING: (Segmentation) Faltan parametros obligatorios (_target, _var_time o var_segmentadora);
            %end;
        %end;
        %else %do;
            %put WARNING: (Segmentation) El dataset no tiene filas;
        %end;

    %end;
    %else %do;
        %put WARNING: (Segmentation) El dataset no existe;
    %end;
%mend;

