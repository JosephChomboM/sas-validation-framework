/*---------------------------------------------------------------------------
  Version: 1.0
  Desarrollador: Joseph Chombo					
  Fecha Release: 03/02/2025
-----------------------------------------------------------------------------*/
%macro __report_model(data, lista_var, target, flg_ponderada=1, hits=1);

	%let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));
	%if &hits = 1 %then %do;
		data t_&rnd._t1;
			set &data.;
			numhits=1;
		run;
	%end;
	%else %do;
		data t_&rnd._t1;
		set &data.;
			numhits=&hits;
		run;
	%end;

	%if &flg_ponderada %then %do;
	    %_reg_log_ponderada(t_&rnd._t1, &lista_var, &target, numhits, get_intercept=1);
	    %_pesos(t_&rnd._t1, t_&rnd._t1_betas, &target, &lista_var);

	    proc sql;
	    create table betas_report as
	    select
	    a.Variable,
	    a.Estimate,
	    a.ProbChiSq,
	    b.peso format= 8.2
	    from t_&rnd._t1_betas a
	    left join pesos_report b on a.Variable = b.Variable
	    ;
	    quit;
		
		title Detail of Model; 
	    	proc print data=t_&rnd._t1_NObs noobs; run;
		title ;
			proc print data=t_&rnd._t1_ResponseProfile noobs; run;
			proc print data=t_&rnd._t1_FitStatistics noobs; run;
			proc print data=betas_report noobs; run;
			proc print data=t_&rnd._t1_stats noobs; run;


	%end;
	    
	%else %do;
	    %_regresion(t_&rnd._t1, &target, &lista_var, get_intercept=1);
	    %_pesos(t_&rnd._t1, t_&rnd._t1_betas, &target, &lista_var);

	    proc sql;
	    create table betas_report as
	    select
	    a.Variable,
	    a.Estimate,
	    a.ProbChiSq,
	    b.peso 
	    from t_&rnd._t1_betas a
	    left join pesos_report b on a.Variable = b.Variable
	    ;    
	    quit;

		title Detail of Model; 
	    	proc print data=t_&rnd._t1_NObs noobs; run;
		title ;
			proc print data=t_&rnd._t1_ResponseProfile noobs; run;
			proc print data=t_&rnd._t1_FitStatistics noobs; run;
			proc print data=betas_report noobs; run;
			proc print data=t_&rnd._t1_stats noobs; run;
	%end;

	proc datasets nolist;
	   delete t_&rnd.: gini betas1 betas2 betas_report pesos_report;
	run;
%mend;

%macro _reg_log_ponderada(tablain, variables, def, weight, get_intercept=0);

	%local rnd;
	%let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

	ods select none;
	proc freq data= &tablain.;
	    table &def. / missing out=t_&rnd._1;
	    weight &weight.;
	run;

	data _null_;
	    set t_&rnd._1; 
	    where &def.=1;
	    multiplier=(100-percent)/percent;
	    call symput("multiplier",left(trim(put(multiplier,20.10))));
	run;

	data t_&rnd._2;
	    set &tablain.;
	    if &def.=1 then sample_w=&weight.*&multiplier.;
	    else sample_w=&weight.;
	run;

	proc logistic data= t_&rnd._2 namelen=45;
	model &def. (event="1")= &variables.;
	weight sample_w;
	output out=&tablain._out pred=y_est;
	ods output NObs = &tablain._nobs;
	ods output ResponseProfile = &tablain._ResponseProfile;
	ods output FitStatistics = &tablain._FitStatistics;
	ods output parameterestimates = &tablain._betas;
	ods output association = &tablain._stats;
	run;  
	ods select all;

	%if &get_intercept = 0 %then %do;
	    data &tablain._betas;
	    set &tablain._betas;
	    where Variable ^= "Intercept";
	    run;
	%end;

	proc datasets nolist;
	   delete t_&rnd.:;
	run;
%mend;

%macro _pesos(data, betas, target, lista_variables);

	%local rnd;
	%let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

	proc sort data=&data. out=t_&rnd._1; by &target.; run;

	ods select none;
	proc means data=t_&rnd._1 stackods; variable &lista_variables.;
	class &target.;
	ods output summary=t_&rnd._2;
	run;

	proc sort data=t_&rnd._2; by Variable; run;
	proc transpose data=t_&rnd._2 prefix=def out=t_&rnd._3(drop=_NAME_); 
	by Variable;
	id &target.;
	var Mean;
	run;

	ods select all;

	data t_&rnd._4;
	set t_&rnd._3;
	diff=abs(def1-def0);
	run;

	proc sql;
	create table t_&rnd._5 as
	select 
	a.Variable,
	a.Estimate,
	b.diff
	from &betas a
	left join t_&rnd._4 b on a.Variable=b.Variable
	where a.Variable not in ("Intercept");
	quit;

	data pesos_report;
	set t_&rnd._5;
	pond= abs(Estimate*diff);
	run;

	proc sql noprint; 
	select sum(pond) into: sum_pond
	from pesos_report;
	quit;

	data pesos_report;
	set pesos_report;
	peso = pond/&sum_pond.;
	run;

	proc datasets nolist;
	   delete t_&rnd.:;
	run;
%mend;

%macro _regresion(tablain, target, lista_variables, get_intercept=0);

    ods select none;
    proc logistic data=&tablain namelen=45;
    model &target.(event="1")= &lista_variables.;
    output out=&tablain._out pred=y_est;
	ods output NObs = &tablain._nobs;
	ods output ResponseProfile = &tablain._ResponseProfile;
	ods output FitStatistics = &tablain._FitStatistics;
	ods output parameterestimates = &tablain._betas;
	ods output association = &tablain._stats;
    run;
    ods select all;

    %if &get_intercept = 0 %then %do;
        data &tablain._betas;
        set &tablain._betas;
        where Variable ^= "Intercept";
        run;
    %end;

%mend;
%macro __supuestos_regresion(dataset=, m_data_type=, target=, vars_num=, time_var=);
    
    proc format;
        value vif_fmt
            0 -< 5 = 'lightgreen'
            5 -< 10 = 'yellow'
            10 - high = 'lightred';
    run;
    ods select none;
    proc reg data=&dataset outest=model_params;
        model &target = &vars_num / vif;
        output out=residuals predicted=predicted residual=residuals student=studentized;
        ods output ParameterEstimates=vif_output;
    run;
    ods select all;
    
    /* -------------- FACTOR DE INFLACION DE VARIANZA -------------- */
    title "Factor de Inflacion de Varianza (VIF) - &m_data_type";
    proc print data=vif_output noobs label;
        var Dependent Variable Estimate StdErr tValue Probt;
        var VarianceInflation / style={background=vif_fmt.};
        where Variable ne 'Intercept';
        label Dependent = "RD" 
               Estimate = "Estimado" 
               StdErr = "Error Std" 
               tValue = "T-value" 
               Probt = "Prob T" 
               VarianceInflation = "VIF";
    run;
    title;
    /* -------------- NORMALIDAD DE RESIDUOS -------------- */
    
    ods select none;
    proc univariate data=residuals normal;
        var residuals;
        histogram residuals / normal;
        qqplot residuals / normal(mu=est sigma=est);
        ods output TestsForNormality=normality_test;
    run;
    ods select all;
    
    title "Normalidad de residuos - &m_data_type";
    proc print data=normality_test noobs; run;
    title;

    /* -------------- IGUALDAD DE VARIANZAS -------------- */
    %_calcular_cortes(residuals, predicted, 10);

    title "Supuesto de Homocedasticidad (Levene) Residuos - &m_data_type";
    proc format;
        value levene_fmt
            0 -< 0.05 = 'lightred'
            0.05 - high = 'lightgreen';
    run;
    proc print data=CORTES noobs;
        var RANGO INICIO FIN ETIQUETA;
    run;
    title;

    ods select none;
    proc glm data=valores_rango_r;
        class RANGO;
        model residuals = RANGO;
        means RANGO / hovtest=levene(type=abs);
        ods output HOVFTest=levene_test; 
    run;
    ods select all;
    
    proc print data=levene_test noobs label;
        where Source ne 'Error';
        var Dependent Method Source SS MS FValue;
        var ProbF / style={background=levene_fmt.};
        label Method = "Metodo" 
               Source = "Fuente" 
               SS = "Suma Cuadr." 
               MS = "Media Cuadr.";
    run;
    
    /* -------------- AUTOCORR DE RESIDUOS -------------- */
    proc format;
    value dw_fmt
        0 -< 1.5 = 'lightred'   
        1.5 -< 2.0 = 'lightgreen'    
        2.0 -< 2.5 = 'lightgreen'    
        2.5 - 4 = 'lightred';
    run;

    %if %length(&time_var) > 0 %then %do;
        proc sort data=residuals;
            by &time_var;
        run;
        ods select none;
        proc autoreg data=residuals;
            model &target = &vars_num / dw=2 dwprob;
            output out=dw_residuals p=p r=r;
            ods output DWTest=dw_test;
        run;
        ods select all;

        title "Autocorrelacion de residuos (Durbin-Watson) - &m_data_type";
        proc print data=dw_test noobs label;
            where Order = 1;
            var Order;
            var DW / style={background=dw_fmt.};
            var ProbDW ProbDWNeg;
            label 
                Order = "Orden"
                ProbDW = "P-value (Autocorr. Positiva)"
                ProbDWNeg = "P-value (Autocorr. Negativa)";
            format DW 8.4 ProbDW ProbDWNeg PVALUE6.4;
        run;
        title;
    %end;   
    
    /* Clean up temporary datasets */
    proc datasets lib=work nolist;
        delete cortes dw_residuals levene_test model_params
        normality_test valores_rango valores_rango_r vif_output
        dw_test residuals;
    quit;
%mend;


%macro _calcular_cortes(tablain, var, groups);

	%local rnd;
	%let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

	data t_&rnd._1;
	/*set &tablain(keep=&var.);*/
	set &tablain;
	&var = put(&var, F12.4);
	run;
	
	PROC RANK DATA=t_&rnd._1 out=t_&rnd._2 GROUPS=&groups;
	RANKS RANGO;
	VAR &var;
	RUN;

	PROC SQL;
	CREATE TABLE valores_rango AS
	SELECT
	RANGO, 
	MIN(&var.) AS MINVAL, 
	MAX(&var.) AS MAXVAL
	FROM t_&rnd._2
	GROUP BY RANGO;
	QUIT;

	PROC SORT DATA=valores_rango; BY RANGO;RUN;

	DATA t_&rnd._4;
	SET valores_rango(RENAME=(RANGO=RANGO_INI)) END=EOF;
	RETAIN MARCA 0;
	N=_n_;
	FLAG_INI=0;
	FLAG_FIN=0;
	LAGMAXVAL=LAG(MAXVAL);
	RANGO=RANGO_INI+1;
	IF RANGO_INI=. THEN RANGO=0;
	IF RANGO_INI>=0 THEN MARCA=MARCA+1;
	IF MARCA=1 THEN FLAG_INI=1;
	IF EOF THEN FLAG_FIN=1;
	RUN;

	PROC SQL;
	CREATE TABLE CORTES AS
	SELECT     "&var." AS VARIABLE LENGTH=32,
	        RANGO, RANGO_INI, LAGMAXVAL AS INICIO, MAXVAL AS FIN, FLAG_INI, FLAG_FIN,
	        CASE WHEN RANGO=0 THEN "00. Missing"    
	            WHEN FLAG_INI=1 THEN CAT(PUT(RANGO,Z2.),". <-Inf; ", cats(PUT(MAXVAL,F12.4)), "]")
	             WHEN FLAG_FIN=1 THEN CAT(PUT(RANGO,Z2.),". <", cats(PUT(LAGMAXVAL,F12.4)), "; +Inf>")
	             ELSE CAT(PUT(RANGO,Z2.), ". <", cats(PUT(LAGMAXVAL,F12.4)), "; ", cats(PUT(MAXVAL,F12.4)), "]") 
	        END AS ETIQUETA LENGTH=200
	FROM t_&rnd._4;
	QUIT;

    PROC SQL;
	CREATE TABLE valores_rango_r AS
	SELECT
    residuals,
	RANGO, 
	MIN(&var.) AS MINVAL, 
	MAX(&var.) AS MAXVAL
	FROM t_&rnd._2
	GROUP BY RANGO;
	QUIT;

	PROC SORT DATA=valores_rango_r; BY RANGO;RUN;

	proc datasets nolist;
	   delete t_&rnd.:;
	run;

%mend;
/*---------------------------------------------------------------------------
  Version: 1.0
  Desarrollador: Joseph Chombo					
  Fecha Release: 06/02/2025
-----------------------------------------------------------------------------*/

%include "&_root_path/Sources/Modulos/m_replica/replica_macro.sas";
%include "&_root_path/Sources/Modulos/m_replica/supuestos_macro.sas";

%macro __replica_report(data=, lista_var=, target=, var_tiempo=, data_type=);
    ods graphics on / outputfmt=svg;
    %if &data_type = TRAIN %then %do;
        /* Iniciar nuevo archivo Excel con hoja para TRAIN */
        ods HTML5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg.ReplicaModelo_1.html";
        ods excel file="&&path_troncal_&tr/Reports/tro_&tr._seg_&seg._Replica.xlsx"
                    options(sheet_name="TRAIN_Replica" 
                        sheet_interval="none" 
                        embedded_titles="yes");
                        
        /* Ejecutar análisis de réplica para TRAIN */
        %__report_model(data=&data, lista_var=&lista_var, target=&target, flg_ponderada=1, hits=1);
        %__supuestos_regresion(dataset=&data, m_data_type=&data_type, target=&target, vars_num=&lista_var, time_var=&var_tiempo);
    ods html5 close;
    %end;
    %else %if &data_type = OOT %then %do;
        /* Agregar hoja OOT al mismo archivo */
        ods HTML5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg.ReplicaModelo_2.html";
        ods excel options(sheet_name="OOT_Replica" 
                        sheet_interval="now" 
                        embedded_titles="yes");

        /* Ejecutar análisis de réplica para OOT */
        %__report_model(data=&data, lista_var=&lista_var, target=&target, flg_ponderada=1, hits=1);
        %__supuestos_regresion(dataset=&data, m_data_type=&data_type, target=&target, vars_num=&lista_var, time_var=&var_tiempo);
     
        /* Cerrar el archivo Excel después de procesar OOT */
        ods excel close;
    ods html5 close;
    %end;
    ods graphics off;
%mend;
%include "&_root_path/Sources/Modulos/m_replica/replica_report.sas";

%macro verify_replica(dataset, data_type=);
    /* Verificar existencia del dataset */
    %if %sysfunc(exist(&dataset)) %then %do;
        %let nobs = %sysfunc(attrn(%sysfunc(open(&dataset)),nobs));

        %if &nobs > 0 %then %do;
            %if %length(&vars_num.) > 0 and  %length(&_target.) > 0 and %length(&_var_time) > 0 %then %do;
                %__replica_report(data=&dataset, lista_var=&vars_num, target=&_target, var_tiempo=&_var_time, data_type=&data_type);
            %end;
            %else %if %length(&vars_num.) > 0 and  %length(&_target.) > 0 %then %do;
                %__replica_report(data=&dataset, lista_var=&vars_num, target=&_target,data_type=&data_type);
            %end;
            %else %do;
                %put WARNING: (Replica) No se definieron variables numericas o el target;
            %end;
        %end;
        %else %do;
            %put WARNING: (Replica) Dataset no tiene filas;
        %end;
    %end;
    %else %do;
        %put WARNING: (Replica) Dataset no existe;
    %end;
%mend;
