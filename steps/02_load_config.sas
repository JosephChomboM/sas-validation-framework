/* =========================================================================
steps/02_load_config.sas - Step 2: Carga/validación de config.sas
Backend del step:
- Inicializa sesión CAS
- Genera run_id
- Carga config.sas (cfg_troncales/cfg_segmentos)
- Crea carpetas de output del run actual (siempre, cada corrida)
========================================================================= */
%include "&fw_root./src/common/log_utils.sas";
%let config_file=&fw_root./config.sas;

%macro _step02_load;
    %local _step_rc;
    %let _step_rc=0;

	/* --- Backend: CAS init + run_id + dirs + log ------------------------ */
	cas conn;
	libname casuser cas caslib=casuser;
	options casdatalimit=ALL;

	data _null_;
		_ts=put(datetime(), E8601DT19.);
		_ts=translate(_ts, "-", ":");
		_ts=compress(_ts, "T");
		call symputx("run_id", cats("run_", _ts));
	run;

    %_create_run_dirs;
    %put NOTE: [step-02] Carpetas de output del run &run_id. creadas.;
    %fw_log_start(step_name=step-02_load_config, run_id=&run_id.,
        fw_root=&fw_root., log_stem=02_load_config);

	%if %sysfunc(fileexist(&config_file.))=0 %then %do;
		%put ERROR: [step-02] No existe config_file=&config_file.;
        %let _step_rc=1;
		%goto _step02_exit;
	%end;
	%else %do;
		%put NOTE: [step-02] config_file=&config_file.;
	%end;

	/* --- Promover tablas de config (necesario para background submit) --- */

	/* Drop promovidas anteriores (si existen) y volver a promover         */
	proc cas;
		session conn;
		table.dropTable / caslib="casuser" name="cfg_troncales" quiet=true;
		table.dropTable / caslib="casuser" name="cfg_segmentos" quiet=true;
	quit;

	%include "&config_file.";

	/* config.sas crea las tablas via DATA step; ahora promoverlas */
	proc casutil;
		promote incaslib="casuser" casdata="cfg_troncales" outcaslib="casuser"
			casout="cfg_troncales";
	quit;

	proc casutil;
		promote incaslib="casuser" casdata="cfg_segmentos" outcaslib="casuser"
			casout="cfg_segmentos";
	quit;

	%put NOTE: [step-02] CAS inicializado, config cargado y promovido.
		run_id=&run_id.;

%_step02_exit:
    %fw_log_stop(step_name=step-02_load_config, step_rc=&_step_rc.);

%mend _step02_load;

/* --- Crear carpetas de output del run (cada corrida) ----------------- */
%macro _create_run_dirs;
	options dlcreatedir;

	libname _mkout1 "&fw_root./outputs";
	libname _mkout1 clear;
	libname _mkout2 "&fw_root./outputs/runs";
	libname _mkout2 clear;

	%let _base=&fw_root./outputs/runs/&run_id.;
	%let _dirs=logs reports images tables manifests experiments;
	%let _nd=%sysfunc(countw(&_dirs., %str( )));

	libname _mkrun "&_base.";
	libname _mkrun clear;

	%do _d=1 %to &_nd.;
		%let _dir=%scan(&_dirs., &_d., %str( ));
		libname _mksub "&_base./&_dir.";
		libname _mksub clear;
	%end;

	options nodlcreatedir;
%mend _create_run_dirs;
%_step02_load;
