/* =========================================================================
fw_prepare_processed.sas - Preparación idempotente de data processed
Lee dataset maestro (raw) y materializa la base persistente completa
por troncal/segmento. TRAIN y OOT se derivan dinámicamente en ejecución.

Usa casuser.cfg_troncales y casuser.cfg_segmentos como fuente de config
(casuser es EXCLUSIVO para config).

Datos operativos usan CASLIBs PATH-based:
RAW  → data/raw/           (lectura del dataset maestro)
PROC → data/processed/     (escritura de base/segNNN, subdirs=1)

Requiere: %fw_path_processed y %_create_caslib, %_save_into_caslib,
%_load_cas_data, %_drop_caslib (cas_utils.sas) ya cargados.

Parámetro opcional:
raw_table= nombre del archivo .sashdat sin extensión (default: mydataset)

design.md §7.3 - Preparación idempotente:
- Crea CASLIBs RAW y PROC.
- Lee raw desde CASLIB RAW, NO desde casuser.
- Sobrescribe outputs en CASLIB PROC.
- Limpia tablas temporales CAS.
- Loggea conteos (nobs) para auditoría mínima.
- No deja tablas operativas en casuser.
========================================================================= */
%macro fw_prepare_processed(raw_table=mydataset);
    %global fw_prepare_processed_rc;
    %local _cfg_has_flag_col _raw_has_flag;
    %let fw_prepare_processed_rc=0;

	%put NOTE:======================================================;
	%put NOTE: [fw_prepare_processed] INICIO - raw_table=&raw_table.;
	%put NOTE:======================================================;

	/* -----------------------------------------------------------------
	0) Crear CASLIBs PATH-based para RAW y PROC
	----------------------------------------------------------------- */
	%_create_caslib( cas_path=&fw_root./data/raw, caslib_name=RAW,
		lib_caslib=RAW, global=Y, cas_sess_name=conn, term_global_sess=0,
		subdirs_flg=0 );

	%_create_caslib( cas_path=&fw_root./data/processed, caslib_name=PROC,
		lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
		subdirs_flg=1 );

	/* Cargar raw desde CASLIB RAW */
	%_load_cas_data( caslib_name=RAW, cas_sess_name=conn, output_data_name=
		&raw_table. );

	/* -----------------------------------------------------------------
	1) Leer cfg_troncales para iterar (config vive en casuser)
	----------------------------------------------------------------- */
    %let _n_troncales=0;
    %if %sysfunc(exist(casuser.cfg_troncales)) = 0 %then %do;
        %put ERROR: [fw_prepare_processed] No existe casuser.cfg_troncales. Ejecute Step 02 y verifique que casuser esté asignado en este step.;
        %let fw_prepare_processed_rc=1;
        %goto _fw_prepare_processed_cleanup;
    %end;

	proc sql noprint;
		select count(*) into :_n_troncales trimmed from casuser.cfg_troncales;
	quit;

    %if %superq(_n_troncales) = %then %do;
        %put ERROR: [fw_prepare_processed] No se pudo resolver el número de troncales desde casuser.cfg_troncales.;
        %let fw_prepare_processed_rc=1;
        %goto _fw_prepare_processed_cleanup;
    %end;
    %if &_n_troncales. = 0 %then %do;
        %put ERROR: [fw_prepare_processed] casuser.cfg_troncales no contiene filas.;
        %let fw_prepare_processed_rc=1;
        %goto _fw_prepare_processed_cleanup;
    %end;

    %let _cfg_has_flag_col=%_fw_ds_hasvar(data=casuser.cfg_troncales, var=flag_tcl);
    %if &_cfg_has_flag_col. = 0 %then %do;
        %put ERROR: [fw_prepare_processed] cfg_troncales debe incluir la columna flag_tcl. Deje el valor vacío si una troncal no requiere filtro por flag.;
        %let fw_prepare_processed_rc=1;
        %goto _fw_prepare_processed_cleanup;
    %end;

	%put NOTE: [fw_prepare_processed] Troncales a procesar: &_n_troncales.;

	data _null_;
		set casuser.cfg_troncales;
		call symputx(cats("_tr_id_", _n_), troncal_id);
		call symputx(cats("_tr_byvar_", _n_), strip(byvar));
        call symputx(cats("_tr_vseg_", _n_), strip(var_seg));
        call symputx(cats("_tr_nseg_", _n_), n_segments);
        call symputx(cats("_tr_flag_", _n_), strip(flag_tcl));
	run;

	/* -----------------------------------------------------------------
	2) Iterar troncales
	----------------------------------------------------------------- */
	%do _t=1 %to &_n_troncales.;

		%let _tid=&&_tr_id_&_t.;
		%let _byvar=&&_tr_byvar_&_t.;
		%let _vseg=&&_tr_vseg_&_t.;
		%let _nseg=&&_tr_nseg_&_t.;
        %let _flag_tcl=&&_tr_flag_&_t.;

		%put NOTE: -----------------------------------------------------;
		%put NOTE: [fw_prepare_processed] Troncal &_tid. (byvar=&_byvar.);
		%put NOTE: -----------------------------------------------------;

        %if %superq(_flag_tcl) ne %then %do;
            proc sql noprint;
                select count(*) into :_raw_has_flag trimmed
                from dictionary.columns
                where upcase(libname) = 'RAW'
                  and upcase(memname) = upcase("&raw_table.")
                  and upcase(name) = upcase("&_flag_tcl.");
            quit;
            %if &_raw_has_flag. = 0 %then %do;
                %put ERROR: [fw_prepare_processed] Troncal &_tid. usa flag_tcl=&_flag_tcl., pero la columna no existe en RAW.&raw_table..;
                %let fw_prepare_processed_rc=1;
                %goto _fw_prepare_processed_cleanup;
            %end;
            %put NOTE: [fw_prepare_processed] Troncal &_tid. filtrada por &_flag_tcl.=1.;
        %end;
        %else %do;
            %put NOTE: [fw_prepare_processed] Troncal &_tid. sin flag_tcl. Se materializa la base completa.;
        %end;

		/* ---- 2a) Crear base persistente completa ----------------------- */
        %fw_path_processed(outvar=_path_base, troncal_id=&_tid.);

        %if %superq(_flag_tcl) ne %then %do;
			data RAW._tmp_base;
				set RAW.&raw_table.(where=(&_flag_tcl. = 1));
			run;
        %end;
        %else %do;
			data RAW._tmp_base;
				set RAW.&raw_table.;
			run;
        %end;

		/* Contar obs para log */
		proc sql noprint;
			select count(*) into :_nobs_base trimmed from RAW._tmp_base;
		quit;
		%put NOTE: [fw_prepare_processed] &_path_base.=> &_nobs_base. obs
            (base completa).;

		%if &_nobs_base.=0 %then %do;
			%put WARNING: [fw_prepare_processed] &_path_base. tiene 0 obs.
				Se crea vacío.;
		%end;

		/* Guardar como .sashdat en CASLIB PROC */
		%_save_into_caslib( m_cas_sess_name=conn, m_input_caslib=RAW,
			m_input_data=_tmp_base, m_output_caslib=PROC, m_subdir_data=
			%sysfunc(tranwrd(&_path_base., .sashdat, )) );

		/* ---- 2b) Segmentos (si aplica) --------------------------------- */
		%if %superq(_vseg) ne and &_nseg. > 0 %then %do;
			%do _sg=1 %to &_nseg.;

				%fw_path_processed(outvar=_path_seg, troncal_id=&_tid.,
					seg_id=&_sg.);

				data RAW._tmp_seg;
					set RAW._tmp_base(where=(&_vseg.=&_sg.));
				run;

				proc sql noprint;
					select count(*) into :_nobs_seg trimmed from RAW._tmp_seg;
				quit;
				%put NOTE: [fw_prepare_processed] &_path_seg.=> &_nobs_seg.
					obs (segmento completo).;

				%_save_into_caslib( m_cas_sess_name=conn, m_input_caslib=RAW,
					m_input_data=_tmp_seg, m_output_caslib=PROC, m_subdir_data=
					%sysfunc(tranwrd(&_path_seg., .sashdat, )) );

				/* Limpiar temporal */
				proc cas;
					table.dropTable / caslib="RAW" name="_tmp_seg"
						quiet=true;
				quit;

			%end; /* segmentos */
		%end;

		/* Limpiar base temporal */
		proc cas;
			table.dropTable / caslib="RAW" name="_tmp_base" quiet=true;
		quit;
	%end; /* troncales */
	/* -----------------------------------------------------------------
	3) Cleanup: tablas temporales en CAS + CASLIBs + macrovariables.
	Los .sashdat persisten en disco; solo liberamos memoria CAS.
	----------------------------------------------------------------- */
%_fw_prepare_processed_cleanup:
	%_drop_caslib(caslib_name=RAW, cas_sess_name=conn, del_prom_tables=1);
	%_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

	%do _t=1 %to &_n_troncales.;
		%symdel _tr_id_&_t. _tr_byvar_&_t. _tr_vseg_&_t. _tr_nseg_&_t.
            _tr_flag_&_t. / nowarn;
	%end;
	%symdel _n_troncales _cfg_has_flag_col _raw_has_flag / nowarn;

	%put NOTE:======================================================;
    %if &fw_prepare_processed_rc. = 0 %then %do;
	%put NOTE: [fw_prepare_processed] FIN;
    %end;
    %else %do;
	%put ERROR: [fw_prepare_processed] FIN con errores.;
    %end;
	%put NOTE:======================================================;

%mend fw_prepare_processed;
