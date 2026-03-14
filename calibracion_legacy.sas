%macro __precision_vars(t1, t2=., target=, prob=, list_var=, list_var_cat=, groups=5, report=1);
    
    %local c v z;
    %let z=1;
    %let v_cat=%scan(&list_var_cat., &z.," ");
    *unir variables numericas y categoricas;
    %do %while(%length(&v_cat)^=0);
         %let list_var= &list_var. &v_cat.#;
         %let z=%eval(&z+1);
         %let v_cat=%scan(&list_var_cat,&z," ");
    %end;
    %let c=1;
    %let v=%scan(&list_var, &c, " ");
    %do %while(%length(&v)^=0);
        %if %substr(&v, %length(&v),1) eq %str(#) %then %do;
            %let v = %substr(&v, 1, %length(&v)-1);	
            %LET DUMMY_LIST=%STR(" ");
              *funcion;
              	%put "&V: ESTOY EN TRAIN PRECISION CATEGORICA";
                %__get_precision(&t1, &v, TARGET=&target, PROB=&prob, 
                       flg_continue=0, groups=&groups, report=&report, 
                       reuse_cuts=0, label_type=TRAIN);
                %if %sysfunc(exist(&t2)) %then %do;
                    %PUT "&V: ESTOY EN OOT PRECISION CATEGORICA";
                    %__get_precision(&t2, &v, TARGET=&target, PROB=&prob, 
                           flg_continue=0, groups=&groups, report=&report, 
                           reuse_cuts=0, label_type=OOT);
                %end;
        %end;
        %else %do;
              *funcion;
                %LET DUMMY_LIST=%STR(., 1111111111, -1111111111, 2222222222, -2222222222, 3333333333, -3333333333, 4444444444, 5555555555, 
6666666666, 7777777777, -999999999);
                %put "&V: ESTOY EN TRAIN PRECISION NUMERICA CORTES 0";
                %__get_precision(&t1, &v, TARGET=&target, PROB=&prob, 
                       flg_continue=1, groups=&groups, report=&report, 
                       reuse_cuts=0, label_type=TRAIN);
                %if %sysfunc(exist(&t2)) %then %do;
                    %PUT "&V: ESTOY EN OOT PRECISION NUMERICA CORTES 1";
                    %__get_precision(&t2, &v, TARGET=&target, PROB=&prob, 
                           flg_continue=1, groups=&groups, report=&report, 
                           reuse_cuts=1, label_type=OOT);
                %end;
        %end;
        %let c=%eval(&c+1);
        %let v=%scan(&list_var, &c, " ");
    %end;

    proc datasets nolist;
        delete cortes;
    run;

%mend;

%MACRO __get_precision(TABIN, VAR, TARGET=, PROB=, FLG_CONTINUE=1, GROUPS=5, FLG_NOMISS=0, reuse_cuts=0, oot=0, report=1, where=., where_oot=., label_type=);

    %local rnd;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

    data t_&rnd._0;
    set &TABIN.;
    if &var in (&dummy_list) then &var=.; /* Handle special values */
    run;

    /* Filter logic if needed */
    %IF &FLG_NOMISS.=1 %THEN %DO;
        DATA t_&rnd._1;
        SET t_&rnd._0;
        %if &where_oot ne . %then %do;
        	WHERE MISSING(&VAR.)=0 and &byvar <= &where_oot;
        %end;
        %else %do;
            WHERE MISSING(&VAR.)=0;
        %end;
        RUN;
    %END;
    %ELSE %DO;
        DATA t_&rnd._1;
        SET t_&rnd._0;
        %if &where_oot ne . %then %do; where &byvar <= &where_oot; %end;
        RUN;
    %END;

    /* Additional filter if needed */
    DATA t_&rnd._1;
    set t_&rnd._1;
    %if &where ne . %then %do; where &where; %end;
    run;

    data _null_;
    if 0 then set t_&rnd._1 nobs=n;
    call symput("total", n);
    stop;
    run;

    %IF &FLG_CONTINUE=1 %THEN %DO;
        %if &reuse_cuts = 0 %then %do;
            %PUT (PRECISION) CALCULANDO NUEVOS CORTES PARA &VAR;
            %_calcular_cortes(t_&rnd._1, &var., &groups);
            proc sort data=cortes; by rango; run;
        %end;
        %else %do;
            %PUT (PRECISION) REUTILIZANDO CORTES PARA &VAR;
            /* Assuming cortes table exists from previous execution */
        %end;

        DATA t_&rnd._2;
            SET cortes END=EOF;
            LENGTH QUERY_START $35 QUERY_END $60;
            N=_n_;
            QUERY_START="WHEN ";
            QUERY_END="";
            IF N=1 THEN QUERY_START="CASE WHEN ";
            IF EOF THEN QUERY_END=" END";
        RUN;

        PROC SQL;
            CREATE TABLE t_&rnd._3 AS
            SELECT     *, 
                CASE WHEN RANGO=0 THEN CAT("IF MISSING(&VAR.)=1 THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                     WHEN FLAG_INI=1 THEN CAT("IF &VAR.<=",FIN," AND &VAR.>. THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                     WHEN FLAG_FIN=1 THEN CAT("IF &VAR.>",INICIO," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                     ELSE CAT("IF ",INICIO,"<&VAR.<=",FIN," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                     END AS QUERY_BODY
            FROM t_&rnd._2;
        QUIT;
        %let dataapply =;
        PROC SQL NOPRINT;
            SELECT QUERY_BODY INTO:DATAAPPLY SEPARATED BY " " FROM t_&rnd._3;
        QUIT;

        DATA t_&rnd._4;
            SET t_&rnd._1;
            LENGTH ETIQUETA $120;
            &DATAAPPLY.;
        RUN;
        
        PROC SQL;
        CREATE TABLE report AS 
        SELECT 
        ETIQUETA AS &VAR,
        COUNT(*) AS N_cuentas, 
        COUNT(*)/&total AS Pct_cuentas format=percent8.0, 
        SUM(case when &TARGET is null then 0 else &TARGET end)/
        SUM(case when &TARGET is null then 0 else 1 end) AS RD format=percent8.2,
        SUM(case when &PROB is null then 0 else &PROB end)/
        SUM(case when &PROB is null then 0 else 1 end) AS PD format=percent8.2
        FROM t_&rnd._4
        GROUP BY ETIQUETA
        ORDER BY ETIQUETA ASC
        ;
        QUIT;

        %_vasicek(report, PD);
        %_vasicek(report, PD, ALPHA=0.25);
        
        title "Precision driver &var. - &label_type";
        PROC SGPLOT DATA=report noautolegend;
            NEEDLE X=&VAR. Y=Pct_cuentas/ LINEATTRS=(COLOR=LIGHTSTEELBLUE THICKNESS=15);
            BAND X=&VAR. LOWER=LI_10 UPPER=LS_10/ FILLATTRS=(COLOR=GOLD) name="a" y2axis;
            BAND X=&VAR. LOWER=LI_25 UPPER=LS_25/ FILLATTRS=(COLOR=BIG) name="b" y2axis;
            SERIES X=&VAR. Y=PD/ LINEATTRS=(COLOR=BLACK THICKNESS=1 PATTERN=DASH) name="c" y2axis; 
            SERIES X=&VAR. Y=RD/ LINEATTRS=(THICKNESS=0) MARKERS MARKERATTRS=(SIZE=8PX SYMBOL=CIRCLEFILLED COLOR=BLUE) name="d" y2axis; 
            yaxis label="% Cuentas (bar)";
            y2axis label="PD (RD blue)" min=0;
            xaxis label="Buckets driver" valueattrs=(size=7);
        RUN;
        title ;
        
        %if &report %then %do;
            proc print data=report noobs; run;
        %end;
    %END;

    %ELSE %DO;
        PROC SQL;
        CREATE TABLE report AS 
        SELECT 
        &VAR,
        COUNT(*) AS N_cuentas, 
        COUNT(*)/&total AS Pct_cuentas format=percent8.0, 
        SUM(case when &TARGET is null then 0 else &TARGET end)/
        SUM(case when &TARGET is null then 0 else 1 end) AS RD format=percent8.2,
        SUM(case when &PROB is null then 0 else &PROB end)/
        SUM(case when &PROB is null then 0 else 1 end) AS PD format=percent8.2
        FROM t_&rnd._1
        GROUP BY &VAR
        ORDER BY &VAR ASC
        ;
        QUIT;

        %_vasicek(report, PD);
        %_vasicek(report, PD, ALPHA=0.25);

        title "Precision driver &var. - &label_type";
        PROC SGPLOT DATA=report noautolegend;
        NEEDLE X=&VAR. Y=Pct_cuentas/ LINEATTRS=(COLOR=LIGHTSTEELBLUE THICKNESS=15);
        BAND X=&VAR. LOWER=LI_10 UPPER=LS_10/ FILLATTRS=(COLOR=GOLD) name="a" y2axis;
        BAND X=&VAR. LOWER=LI_25 UPPER=LS_25/ FILLATTRS=(COLOR=BIG) name="b" y2axis;
        SERIES X=&VAR. Y=PD/ LINEATTRS=(COLOR=BLACK THICKNESS=1 PATTERN=DASH) name="c" y2axis; 
        SERIES X=&VAR. Y=RD/ LINEATTRS=(THICKNESS=0) MARKERS MARKERATTRS=(SIZE=8PX SYMBOL=CIRCLEFILLED COLOR=BLUE) name="d" y2axis; 
        yaxis label="% Cuentas (bar)";
        y2axis label="PD (RD blue)" min=0;
        xaxis label="Buckets driver" valueattrs=(size=7) type=discrete;
        RUN;
        title;

        %if &report %then %do;
            proc print data=report noobs; run;
        %end;
    %END;

    proc datasets nolist;
       delete t_&rnd.: report;
    run;
    
    /* Clean up cortes only if OOT is done */
    %if &reuse_cuts = 1 %then %do;
        proc datasets nolist;
            delete cortes;
        run;
    %end;

%MEND;

/* Other macros remain unchanged */

%MACRO _vasicek(DATAIN, EST, RHO=0.005, ALPHA=0.1);
	%local n;
	%let n = %sysevalf(100*&alpha);

	DATA &DATAIN.;
	SET &DATAIN.;
	FORMAT LI_&N percent8.2 LS_&N percent8.2;
	ARG1 = ((1-&RHO)**(-0.5))*QUANTILE("NORMAL", &EST);
	ARG2 = ((&RHO/(1-&RHO))**(0.5))*(QUANTILE("NORMAL", &ALPHA));
	ARG3 = ((&RHO/(1-&RHO))**(0.5))*(QUANTILE("NORMAL", 1-&ALPHA));
	LI_&N = CDF("NORMAL", ARG1+ARG2);
	LS_&N = CDF("NORMAL", ARG1+ARG3);
	DROP ARG1 ARG2 ARG3;
	RUN;
%MEND;

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
	CREATE TABLE t_&rnd._3 AS
	SELECT 
	RANGO, 
	MIN(&var.) AS MINVAL, 
	MAX(&var.) AS MAXVAL
	FROM t_&rnd._2
	GROUP BY RANGO;
	QUIT;

	PROC SORT DATA=t_&rnd._3; BY RANGO;RUN;

	DATA t_&rnd._4;
	SET t_&rnd._3(RENAME=(RANGO=RANGO_INI)) END=EOF;
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

	proc datasets nolist;
	   delete t_&rnd.:;
	run;

%mend;
%macro __precision_vars_ponderado(t1, t2=., target=, prob=, monto=, byvarlocal=, lista_var=, lista_var_cat=, groups=5, report=1);
    
    %local c v z;
    %let z=1;
    %let v_cat=%scan(&lista_var_cat., &z.," ");
    *unir variables numericas y categoricas;
    %do %while(%length(&v_cat)^=0);
         %let lista_var= &lista_var. &v_cat.#;
         %let z=%eval(&z+1);
         %let v_cat=%scan(&lista_var_cat,&z," ");
    %end;
    %let c=1;
    %let v=%scan(&lista_var, &c, " ");
    %do %while(%length(&v)^=0);
        %if %substr(&v, %length(&v),1) eq %str(#) %then %do;
            %let v = %substr(&v, 1, %length(&v)-1);	
            %LET DUMMY_LIST=%STR(" ");
              *funcion;
              	%put "&V: ESTOY EN TRAIN PRECISION POND CATEGORICA";
                %__get_precision_ponderado(&t1, &v, TARGET=&target, PROB=&prob, MONTO=&monto, 
                       flg_continue=0, groups=&groups, report=&report, 
                       reuse_cuts=0, label_type=TRAIN);
                %if %sysfunc(exist(&t2)) %then %do;
                    %PUT "&V: ESTOY EN OOT PRECISION POND CATEGORICA";
                    %__get_precision_ponderado(&t2, &v, TARGET=&target, PROB=&prob, MONTO=&monto, 
                           flg_continue=0, groups=&groups, report=&report, 
                           reuse_cuts=0, label_type=OOT);
                %end;
        %end;
        %else %do;
              *funcion;
                %LET DUMMY_LIST=%STR(., 1111111111, -1111111111, 2222222222, -2222222222, 3333333333, -3333333333, 4444444444, 5555555555, 
6666666666, 7777777777, -999999999);
                %put "&V: ESTOY EN TRAIN PRECISION POND NUMERICA CORTES 0";
                %__get_precision_ponderado(&t1, &v, TARGET=&target, PROB=&prob, MONTO=&monto, 
                       flg_continue=1, groups=&groups, report=&report, 
                       reuse_cuts=0, label_type=TRAIN);
                %if %sysfunc(exist(&t2)) %then %do;
                    %PUT "&V: ESTOY EN OOT PRECISION POND NUMERICA CORTES 1";
                    %__get_precision_ponderado(&t2, &v, TARGET=&target, PROB=&prob, MONTO=&monto, 
                           flg_continue=1, groups=&groups, report=&report, 
                           reuse_cuts=1, label_type=OOT);
                %end;
        %end;
        %let c=%eval(&c+1);
        %let v=%scan(&lista_var, &c, " ");
    %end;

    proc datasets nolist;
        delete cortes;
    run;

%mend;

%MACRO __get_precision_ponderado(TABIN, VAR, TARGET=, PROB=, MONTO=, FLG_CONTINUE=1, GROUPS=5, FLG_NOMISS=0, reuse_cuts=0, oot=0, report=1, where=., where_oot=., label_type=);

    %local rnd;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

    data t_&rnd._0;
    set &TABIN.;
    if &var in (&dummy_list) then &var=.; /* Handle special values */
    run;

    /* Filter logic if needed */
    %IF &FLG_NOMISS.=1 %THEN %DO;
        DATA t_&rnd._1;
        SET t_&rnd._0;
        %if &where_oot ne . %then %do;
        	WHERE MISSING(&VAR.)=0 and &byvar <= &where_oot;
        %end;
        %else %do;
            WHERE MISSING(&VAR.)=0;
        %end;
        RUN;
    %END;
    %ELSE %DO;
        DATA t_&rnd._1;
        SET t_&rnd._0;
        %if &where_oot ne . %then %do; where &byvar <= &where_oot; %end;
        RUN;
    %END;

    /* Additional filter if needed */
    DATA t_&rnd._1;
    set t_&rnd._1;
    %if &where ne . %then %do; where &where; %end;
    run;

    data _null_;
    if 0 then set t_&rnd._1 nobs=n;
    call symput("total", n);
    stop;
    run;

    %IF &FLG_CONTINUE=1 %THEN %DO;
        %if &reuse_cuts = 0 %then %do;
            %PUT (PRECISION POND) CALCULANDO NUEVOS CORTES PARA &VAR;
            %_calcular_cortes(t_&rnd._1, &var., &groups);
            proc sort data=cortes; by rango; run;
        %end;
        %else %do;
            %PUT (PRECISION POND) REUTILIZANDO CORTES PARA &VAR;
            /* Assuming cortes table exists from previous execution */
        %end;

        DATA t_&rnd._2;
            SET cortes END=EOF;
            LENGTH QUERY_START $35 QUERY_END $60;
            N=_n_;
            QUERY_START="WHEN ";
            QUERY_END="";
            IF N=1 THEN QUERY_START="CASE WHEN ";
            IF EOF THEN QUERY_END=" END";
        RUN;

        PROC SQL;
            CREATE TABLE t_&rnd._3 AS
            SELECT     *, 
                CASE WHEN RANGO=0 THEN CAT("IF MISSING(&VAR.)=1 THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                     WHEN FLAG_INI=1 THEN CAT("IF &VAR.<=",FIN," AND &VAR.>. THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                     WHEN FLAG_FIN=1 THEN CAT("IF &VAR.>",INICIO," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                     ELSE CAT("IF ",INICIO,"<&VAR.<=",FIN," THEN ETIQUETA=",'"',STRIP(ETIQUETA),'";')
                     END AS QUERY_BODY
            FROM t_&rnd._2;
        QUIT;
        %let dataapply =;
        PROC SQL NOPRINT;
            SELECT QUERY_BODY INTO:DATAAPPLY SEPARATED BY " " FROM t_&rnd._3;
        QUIT;

        DATA t_&rnd._4;
            SET t_&rnd._1;
            LENGTH ETIQUETA $120;
            &DATAAPPLY.;
        RUN;
        
        PROC SQL;
        CREATE TABLE report_temp AS 
        SELECT 
        ETIQUETA AS &VAR,
        COUNT(*) AS N_cuentas, 
        SUM(case when &TARGET. is null or &MONTO. is null  then 0 else 1 end) as registros_RD,
        SUM(case when &PROB. is null or &MONTO. is null  then 0 else 1 end) as registros_PD,
        COUNT(*)/&total AS Pct_cuentas format=percent8.0, 
        SUM(case when &TARGET. is null or &MONTO. is null  then 0 else &TARGET. * &MONTO. end) as divisor,
        SUM(case when &TARGET. is null or &MONTO. is null  then 0 else &MONTO. end) as dividendo,
        SUM(case when &TARGET. is not null and &MONTO. is not null then 1 else 0 end) as Registros_RD_pond,
        SUM(case when &PROB. is null or &MONTO. is null  then 0 else &PROB. * &MONTO. end) as divisor2,
        SUM(case when &PROB. is null or &MONTO. is null then 0 else &MONTO. end) as dividendo2,
        SUM(case when &PROB. is not null and &MONTO. is not null then 1 else 0 end) as Registros_PD_pond
        FROM t_&rnd._4
        GROUP BY ETIQUETA
        ORDER BY ETIQUETA ASC
        ;
        QUIT;
    
        PROC SQL;
        CREATE TABLE report as	
        SELECT &VAR , N_cuentas, Pct_cuentas,registros_RD,registros_PD,  Registros_RD_pond, Registros_PD_pond,
        divisor/dividendo as RD format=percent8.2,
        divisor2/dividendo2 as PD format=percent8.2
        FROM  report_temp;
        QUIT;

        %_vasicek(report, PD);
        %_vasicek(report, PD, ALPHA=0.25);
        
        title "Precision driver Pond. &var. - &label_type";
        PROC SGPLOT DATA=report noautolegend;
            NEEDLE X=&VAR. Y=Pct_cuentas/ LINEATTRS=(COLOR=LIGHTSTEELBLUE THICKNESS=15);
            BAND X=&VAR. LOWER=LI_10 UPPER=LS_10/ FILLATTRS=(COLOR=GOLD) name="a" y2axis;
            BAND X=&VAR. LOWER=LI_25 UPPER=LS_25/ FILLATTRS=(COLOR=BIG) name="b" y2axis;
            SERIES X=&VAR. Y=PD/ LINEATTRS=(COLOR=BLACK THICKNESS=1 PATTERN=DASH) name="c" y2axis; 
            SERIES X=&VAR. Y=RD/ LINEATTRS=(THICKNESS=0) MARKERS MARKERATTRS=(SIZE=8PX SYMBOL=CIRCLEFILLED COLOR=BLUE) name="d" y2axis; 
            yaxis label="% Cuentas (bar)";
            y2axis label="PD (RD blue)" min=0;
            xaxis label="Buckets driver" valueattrs=(size=7);
        RUN;
        title ;
        
        %if &report %then %do;
            proc print data=report noobs; run;
        %end;
    %END;

    %ELSE %DO;
        PROC SQL;
        CREATE TABLE report_temp AS 
        SELECT
        &VAR.,
        COUNT(*) AS N_cuentas,
        SUM(case when &TARGET. is null or &MONTO. is null  then 0 else 1 end) as registros_RD,
        SUM(case when &PROB. is null or &MONTO. is null  then 0 else 1 end) as registros_PD,
        COUNT(*)/&total AS Pct_cuentas format=percent8.0, 
        SUM(case when &TARGET. is null or &MONTO. is null  then 0 else &TARGET. * &MONTO. end) as divisor,
        SUM(case when &TARGET. is null or &MONTO. is null then 0 else &MONTO. end) as dividendo,
        SUM(case when &TARGET. is null or &MONTO. is null then 0 else 1 end) as Registros_RD_pond,
        SUM(case when &PROB. is null or &MONTO. is null  then 0 else &PROB. * &MONTO. end) as divisor2,
        SUM(case when &PROB. is null or &MONTO. is null then 0 else &MONTO. end) as dividendo2,
        SUM(case when &PROB. is not null and &MONTO. is not null then 1 else 0 end) as Registros_PD_pond
        FROM t_&rnd._1
        GROUP BY &VAR.
        ORDER BY &VAR. ASC
        ;
        QUIT;

        PROC SQL;
        CREATE TABLE report AS
        SELECT &VAR. , N_cuentas, Pct_cuentas,registros_RD,registros_PD,Registros_RD_pond,Registros_PD_pond,
        divisor/dividendo as RD format=percent8.2,
        divisor2/dividendo2 as PD format=percent8.2
        FROM  report_temp;
        QUIT;

        %_vasicek(report, PD);
        %_vasicek(report, PD, ALPHA=0.25);

        title "Precision driver Pond. &var. - &label_type";
        PROC SGPLOT DATA=report noautolegend;
        NEEDLE X=&VAR. Y=Pct_cuentas/ LINEATTRS=(COLOR=LIGHTSTEELBLUE THICKNESS=15);
        BAND X=&VAR. LOWER=LI_10 UPPER=LS_10/ FILLATTRS=(COLOR=GOLD) name="a" y2axis;
        BAND X=&VAR. LOWER=LI_25 UPPER=LS_25/ FILLATTRS=(COLOR=BIG) name="b" y2axis;
        SERIES X=&VAR. Y=PD/ LINEATTRS=(COLOR=BLACK THICKNESS=1 PATTERN=DASH) name="c" y2axis; 
        SERIES X=&VAR. Y=RD/ LINEATTRS=(THICKNESS=0) MARKERS MARKERATTRS=(SIZE=8PX SYMBOL=CIRCLEFILLED COLOR=BLUE) name="d" y2axis; 
        yaxis label="% Cuentas (bar)";
        y2axis label="PD (RD blue)" min=0;
        xaxis label="Buckets driver" valueattrs=(size=7) type=discrete;
        RUN;
        title;

        %if &report %then %do;
            proc print data=report noobs; run;
        %end;
    %END;

    proc datasets nolist;
       delete t_&rnd.: report report_temp;
    run;
    
    /* Clean up cortes only if OOT is done */
    %if &reuse_cuts = 1 %then %do;
        proc datasets nolist;
            delete cortes;
        run;
    %end;

%MEND;

/* Keep _vasicek and _calcular_cortes macros as they are */
%MACRO _vasicek(DATAIN, EST, RHO=0.005, ALPHA=0.1);
	%local n;
	%let n = %sysevalf(100*&alpha);

	DATA &DATAIN.;
	SET &DATAIN.;
	FORMAT LI_&N percent8.2 LS_&N percent8.2;
	ARG1 = ((1-&RHO)**(-0.5))*QUANTILE("NORMAL", &EST);
	ARG2 = ((&RHO/(1-&RHO))**(0.5))*(QUANTILE("NORMAL", &ALPHA));
	ARG3 = ((&RHO/(1-&RHO))**(0.5))*(QUANTILE("NORMAL", 1-&ALPHA));
	LI_&N = CDF("NORMAL", ARG1+ARG2);
	LS_&N = CDF("NORMAL", ARG1+ARG3);
	DROP ARG1 ARG2 ARG3;
	RUN;
%MEND;

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
	CREATE TABLE t_&rnd._3 AS
	SELECT 
	RANGO, 
	MIN(&var.) AS MINVAL, 
	MAX(&var.) AS MAXVAL
	FROM t_&rnd._2
	GROUP BY RANGO;
	QUIT;

	PROC SORT DATA=t_&rnd._3; BY RANGO;RUN;

	DATA t_&rnd._4;
	SET t_&rnd._3(RENAME=(RANGO=RANGO_INI)) END=EOF;
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

	proc datasets nolist;
	   delete t_&rnd.:;
	run;

%mend;
%include "&_root_path/Sources/Modulos/m_calibracion/BacktestingDrivers_macro.sas";
%include "&_root_path/Sources/Modulos/m_calibracion/BacktestingDriversPond_macro.sas";

%macro __precision_report(train_data=, oot_data=., r_target=, r_var_pd=, r_vars_dri_num=, r_vars_dri_cat=);
    /* Iniciar archivo Excel para precision */
	
	ods graphics on / outputfmt=svg;
	ods HTML5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Calibracion_1.html";
    ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Backtesting_Precision.xlsx"
            options(sheet_name="TRAIN OOT Backtesting_Precision" 
                    sheet_interval="none" 
                    embedded_titles="yes");
    
    /* Process both train and OOT with one call */
    %__precision_vars(&train_data, t2=&oot_data, 
                      target=&r_target, prob=&r_var_pd, 
                      list_var=&r_vars_dri_num, list_var_cat=&r_vars_dri_cat,
                      groups=5, report=1);
                      
	ods html5 close;
    ods excel close;
%mend;

%macro __precision_ponderado_report(train_data=, oot_data=., r_target=, r_var_pd=, r_monto=, r_time=, r_vars_dri_num=, r_vars_dri_cat=);
    /* Iniciar archivo Excel para precision ponderado */

	ods graphics on / outputfmt=svg;
	ods HTML5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Calibracion_pond_1.html";
    ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Backtesting_PrecisionPonderado.xlsx"
            options(sheet_name="TRAIN OOT Precision_Ponderado" 
                    sheet_interval="none" 
                    embedded_titles="yes");
    
    /* Process both train and OOT with one call */
    %__precision_vars_ponderado(&train_data, t2=&oot_data, 
                                target=&r_target, prob=&r_var_pd,
                                monto=&r_monto, byvarlocal=&r_time, 
                                lista_var=&r_vars_dri_num,
                                lista_var_cat=&r_vars_dri_cat,
                                groups=5, report=1);
                            
	ods html5 close;    
    ods excel close;
%mend;
%include "&_root_path/Sources/Modulos/m_calibracion/Backtesting_report.sas";

%Macro verify_precision(dataset1, dataset2);
    %let ds1_exists = %sysfunc(exist(&dataset1));
    %let ds2_exists = %sysfunc(exist(&dataset2));
    %let target_exists = %length(&_target) > 0;
    %let pd_exists = %length(&var_pd) > 0;
    
    %let nobs1 = 0;
    %let nobs2 = 0;
    
    %if &ds1_exists %then %do;
        %let nobs1 = %sysfunc(attrn(%sysfunc(open(&dataset1)),nobs));
        %if &nobs1 = 0 %then %do;
            %put WARNING: (Backtesting) Dataset &dataset1 existe con 0 obs;
        %end;
    %end;
    %else %do;
        %put WARNING: (Backtesting) Dataset &dataset1 no existe;
    %end;
    
    %if &ds2_exists %then %do;
        %let nobs2 = %sysfunc(attrn(%sysfunc(open(&dataset2)),nobs));
        %if &nobs2 = 0 %then %do;
            %put WARNING: (Backtesting) Dataset &dataset2 existe con 0 obs;
        %end;
    %end;
    %else %do;
        %put WARNING: (Backtesting) Dataset &dataset2 no existe;
    %end;
    
    %let exist_driver = 0;
    %if %length(&vars_dri_num) > 0 or %length(&vars_dri_cat) > 0 %then %do;
        %let exist_driver = 1;
    %end;
    
    %if not &target_exists %then %do;
        %put WARNING: (Backtesting) No se encontro el target. No se puede ejecutar;
    %end;
    
    %if not &pd_exists %then %do;
        %put WARNING: (Backtesting) No se encontro la variable PD. No se puede ejecutar;
    %end;
    
    %if &target_exists and &pd_exists and (&nobs1 > 0 or &nobs2 > 0) %then %do;
        %if &nobs1 > 0 %then %do;
            %let use_ds1 = &dataset1;
        %end;
        %else %do;
            %let use_ds1 = .;
        %end;
        
        %if &nobs2 > 0 %then %do;
            %let use_ds2 = &dataset2;
        %end;
        %else %do;
            %let use_ds2 = .;
        %end;
        
        /* Execute precision report if we have drivers */
        %if &exist_driver = 1 %then %do;
            %put (Backtesting) Procesando precision para los drivers;
            %__precision_report(train_data=&use_ds1, oot_data=&use_ds2, 
                            r_target=&_target, r_var_pd=&var_pd,
                            r_vars_dri_num=&vars_dri_num, r_vars_dri_cat=&vars_dri_cat);
            
            /* Check if we can do weighted analysis */
            %if %length(&monto) > 0 and %length(&_var_time) > 0 %then %do;
                %put (Backtesting) Procesando precision ponderada para los drivers;
                %__precision_ponderado_report(train_data=&use_ds1, oot_data=&use_ds2,
                                        r_target=&_target, r_var_pd=&var_pd,
                                        r_monto=&monto, r_time=&_var_time,
                                        r_vars_dri_num=&vars_dri_num, r_vars_dri_cat=&vars_dri_cat);
            %end;
            %else %do;
                %put WARNING: (Backtesting) No se ejecutará el análisis ponderado porque falta la variable monto o tiempo;
            %end;
        %end;
        %else %do;
            %put WARNING: (Backtesting) No existen drivers definidos;
        %end;
    %end;
    %else %do;
        %put WARNING: (Backtesting) No hay datasets validos con obs o faltan variables requeridas;
    %end;
%Mend;