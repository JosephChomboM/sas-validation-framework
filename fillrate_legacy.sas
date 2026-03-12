%include "&_root_path/Sources/Modulos/m_gini/__aux_gini_utils.sas";

%macro __fillrate_general(table, lista_var,target_param ,OOT=0);
	%local rnd;
	%let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));
	%local c v;
	%let c=1;
	
	data t_&rnd._1;
		length Variable $40 Fillrate 8. GINI 8.;
		format Variable 
char40. Fillrate 8.4 gini best8.4;
		stop;
	run;
    %if &lista_var. ne  %then %do;
		%LET DUMMY_LIST=%STR(., 1111111111, -1111111111, 2222222222, -2222222222, 3333333333, -3333333333, 4444444444, 5555555555, 
		6666666666, 7777777777, -999999999);
        
        %let v=%scan(&lista_var, &c, " ");

        %do %while(%length(&v)^=0);
			%put variable &v;
            proc sql;
                create table t_&rnd._2 as
                select 
                /* Calcula el fillrate general */
                "%upcase(&v.)" as Variable,
                (sum(case when &v. not in (&dummy_list) then 1 else 0 end) / count(*)) * 100 as Fillrate
                from &table.;
            quit;
            %__gini_calc(data=&table., target=&target_param, var=&v, out_gini=_gini_temp);
            proc sql;
                insert into t_&rnd._1
                select 
					a.Variable,a.Fillrate ,
					&_gini_temp. as gini
				from t_&rnd._2 as a;  
            quit;
        
            %let c=%eval(&c+1);
            %let v=%scan(&lista_var, &c, " ");
        %end;

    %end;

    
	*%OOT_TEXT;

	proc print data=t_&rnd._1 label noobs;
		label Fillrate = 'Fillrate_(%)';
		title "Fillrate vs Gini";
	run;
	
	proc datasets nolist;
		delete t_&rnd.: gini;
	run;
%mend;

%macro __fillrate_mensual(table,paramByvar, lista_var= ,lista_var_cat= ,t=);
	*Calcula el fillrate por variable tiempo para variables categoricas y numericas;

	%local rnd;
	%let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));
	%local c v;
	%let c=1;
	*fillrate variables numericas;
	%if &lista_var. ne  %then %do;
		%LET DUMMY_LIST=%STR(., 1111111111, -1111111111, 2222222222, -2222222222, 3333333333, -3333333333, 4444444444, 5555555555, 
		6666666666, 7777777777, -999999999);

		%let v=%scan(&lista_var, &c, " ");
			%do %while(%length(&v)^=0); 
				%put variable numericas &v;
				proc sql;
					create table t_&rnd._1 as
					select 
					&paramByvar.,
					/* Calcula el fillrate */
					(sum(case when &v. not in (&dummy_list) then 1 else 0 end) / count(*)) * 100 as Fillrate
					from &table.
					group by &paramByvar.;
				quit;
				/* Paso 2: IMPRIMIR filrrate por codmes*/
		
				proc print data=t_&rnd._1 label noobs;
					label Fillrate = 'Fillrate_(%)';
					title "Fillrate por &paramByvar de &v. ";
				run;
	
				/* Paso 3: gráfico para interpretar el fillrate */
				proc sgplot data=t_&rnd._1;
					vline &paramByvar./ response=Fillrate lineattrs=(color=crimson) stat=mean;
					xaxis label="&paramByvar" ;
					yaxis label='Fillrate (%)' min=0 max=100 ;
					title "Fillrate de &v. por &paramByvar ";
				run;
			%let c=%eval(&c+1);
			%let v=%scan(&lista_var, &c, " ");
		%end;
	%end;

	*fillrate variables categoricas;
	%let c=1;
	%if &lista_var_cat. ne  %then %do;
		%put NOTE: Procesando lista de variables categoricas;
		%let v=%scan(&lista_var_cat, &c, " ");

			%do %while(%length(&v)^=0); 
				%put variable categoricas &v;
				proc sql;
					create table t_&rnd._1 as
					select 
					&paramByvar.,
					/* Calcula el fillrate */
					(sum(case when not missing(&v.) then 1 else 0 end) / count(*)) * 100 as Fillrate
					from &table.
					group by &paramByvar.;
				quit;
				/* Paso 2: IMPRIMIR filrrate por codmes*/
		
				proc print data=t_&rnd._1 label noobs;
					label Fillrate = 'Fillrate_(%)';
					title "Fillrate por &paramByvar de &v. ";
				run;
	
				/* Paso 3: gráfico para interpretar el fillrate */
				proc sgplot data=t_&rnd._1;
					vline &paramByvar./ response=Fillrate lineattrs=(color=crimson) stat=mean;
					xaxis label="&paramByvar" ;
					yaxis label='Fillrate (%)' min=0 max=100 ;
					title "Fillrate de &v. por &paramByvar ";
				run;
			%let c=%eval(&c+1);
			%let v=%scan(&lista_var_cat, &c, " ");
		%end;
	%end;

	proc datasets nolist;
		delete t_&rnd.:;
	run;

%mend;
%include "&_root_path/Sources/Modulos/m_fillrate/fillrate_macro.sas";

%macro __fillrate_report(dataset=, paramByvar=, lista_var=, lista_var_cat=, _t=, target_param=, run_general=0, data_type=);


	ods graphics on / outputfmt=svg;
    %if &data_type = TRAIN %then %do;
		ods html5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Fillrate_1.html";		
        ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Fillrate.xlsx"
                 options(sheet_name="TRAIN_Fillrate" sheet_interval="none" embedded_titles="yes");
                 
        /* Ejecuta fillrateGeneral si el parámetro run_general=1 */
        %if &run_general = 1 %then %do;
            title "TRAIN: Fillrate General";
            %__fillrate_general(table=&dataset,
                            lista_var=&lista_var,
                            target_param=&target_param);
            title;
        %end;
        
        /* Ejecuta fillrateMensual */
        title "TRAIN: Fillrate Mensual";
        %__fillrate_mensual(table=&dataset, 
                            paramByvar=&paramByvar, 
                            lista_var=&lista_var, 
                            lista_var_cat=&lista_var_cat);
        title;
		ods html5 close;
    %end;
    %else %if &data_type = OOT %then %do;

		ods html5 file="&&path_troncal_&tr/&_img_path/tro_&tr._seg_&seg._Fillrate_2.html";	
        ods excel options(sheet_name="OOT_Fillrate" sheet_interval="now" embedded_titles="yes");
        /* Ejecuta fillrateGeneral si el parámetro run_general=1 */
        %if &run_general = 1 %then %do;
            title "OOT: Fillrate General";
            %__fillrate_general(table=&dataset,
                            lista_var=&lista_var,
                            target_param=&target_param);
            title;
        %end;
        
        /* Ejecuta fillrateMensual */
        title "OOT: Fillrate Mensual";
        %__fillrate_mensual(table=&dataset, 
                            paramByvar=&paramByvar, 
                            lista_var=&lista_var, 
                            lista_var_cat=&lista_var_cat);
        title;
        
        ods excel close;
		ods html5 close;
    %end;
	ods graphics off;
%mend;
%include "&_root_path/Sources/Modulos/m_fillrate/fillrate_report.sas";

%macro verify_fillrate(dataset, data_type=);
    %if %sysfunc(exist(&dataset)) and %length(&_var_time.) > 0 %then %do;
        %local run_general;
        %let run_general = 0;
        
        /* Verificar si se puede ejecutar fillrate_general */
        %if %length(&vars_num.) > 0 and %length(&_target.) > 0 %then %do;
            %let run_general = 1;
        %end;
        
        %if %length(&vars_cat.) > 0 or %length(&vars_num.) > 0 %then %do;
            %if %length(&vars_cat.) > 0 and %length(&vars_num.) > 0 %then %do;
                %__fillrate_report(dataset=&dataset, 
                                paramByvar=&_var_time., 
                                lista_var=&vars_num., 
                                lista_var_cat=&vars_cat., 
                                target_param=&_target.,
                                run_general=&run_general,
                                data_type=&data_type);
            %end;
            %else %if %length(&vars_cat.) > 0 %then %do;
                %__fillrate_report(dataset=&dataset, 
                                paramByvar=&_var_time., 
                                lista_var_cat=&vars_cat., 
                                target_param=&_target.,
                                run_general=0,
                                data_type=&data_type); /* No hay variables numéricas */
            %end;
            %else %if %length(&vars_num.) > 0 %then %do;
                %__fillrate_report(dataset=&dataset, 
                                paramByvar=&_var_time., 
                                lista_var=&vars_num., 
                                target_param=&_target.,
                                run_general=&run_general,
                                data_type=&data_type);
            %end;
        %end;
        %else %do;
            %put WARNING: (FillrateMensual) No existen variables numéricas ni categóricas;
        %end;
    %end;
    %else %do;
         %put WARNING: (FillrateMensual) No se pudo ejecutar porque falta dataset o variable tiempo(_paramByvar);
    %end;
%mend;/*==============================================================
SECCION 1: MACROS BASE DE CALCULO
==============================================================*/

/*--------------------------------------------------------------
Macro: __gini_calc
Descripción: Cálculo base del coeficiente GINI (Somers' D)
Output: Macro variable &out_gini
--------------------------------------------------------------*/
%macro __gini_calc(data=, target=, var=, out_gini=gini_valor);

    %global &out_gini.;
    %let &out_gini.=.;

    %local rnd dsid rc;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* Verificar que el dataset existe */
    %if not %sysfunc(exist(&data.)) %then %do;
        %put WARNING: [GINI] Dataset no existe: &data.;
        %return;
    %end;

    /* Verificar que las variables existen */
    %let dsid=%sysfunc(open(&data.));
    %if &dsid. > 0 %then %do;
        %if %sysfunc(varnum(&dsid., &target.))=0 %then %do;
            %put WARNING: [GINI] Variable target no existe: &target.;
            %let rc=%sysfunc(close(&dsid.));
            %return;
        %end;
        %if %sysfunc(varnum(&dsid., &var.))=0 %then %do;
            %put WARNING: [GINI] Variable score no existe: &var.;
            %let rc=%sysfunc(close(&dsid.));
            %return;
        %end;
        %let rc=%sysfunc(close(&dsid.));
    %end;

    /* Intentar PROC LOGISTIC con ridging por defecto */
    ods select none;

    proc logistic data=&data.;
        model &target. (event="1")=&var.;
        ods output association=_gini_assoc_&rnd.;
    run;
    ods select all;

    /* Si falla, reintentar con RIDGING=NONE */
    %if not %sysfunc(exist(_gini_assoc_&rnd.)) %then %do;
        %put NOTE: [GINI] Reintentando &var. con RIDGING=NONE;
        ods select none;

        proc logistic data=&data.;
            model &target. (event="1")=&var. / RIDGING=NONE;
            ods output association=_gini_assoc_&rnd.;
        run;
        ods select all;
    %end;

    /* Extraer Somers' D (GINI) */
    %if %sysfunc(exist(_gini_assoc_&rnd.)) %then %do;
        proc sql noprint;
            select abs(nvalue2) into :&out_gini. trimmed from _gini_assoc_&rnd.
                where substr(label2, 1, 6)="Somers";
        quit;
    %end;
    %else %do;
        %put WARNING: [GINI] LOGISTIC falló para variable &var.;
    %end;

    /* Limpiar */
    proc datasets lib=work nolist nowarn;
        delete _gini_assoc_&rnd.;
    quit;

%mend __gini_calc;
