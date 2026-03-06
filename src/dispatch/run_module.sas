/* =========================================================================
run_module.sas - Dispatch: ejecuta un modulo en un contexto dado

Dos modos de operacion:

A) Single-input (dual_input=0, default):
Ciclo: resolver path -> promote _active_input -> ejecutar -> drop.
Para modulos que operan sobre un solo dataset (correlacion, gini, etc.).
El modulo recibe: input_table=_active_input, split=<train|oot>.

B) Dual-input (dual_input=1):
Ciclo: resolver paths train+oot -> promote ambas -> ejecutar -> drop.
Para modulos que comparan TRAIN vs OOT simultaneamente (PSI, etc.).
El modulo recibe: train_table=_train_input, oot_table=_oot_input.
El parametro split= se ignora (siempre promueve ambos).

Parametros:
module     = nombre del modulo (correlacion, psi, gini, etc.)
troncal_id = id numerico de troncal
split      = train | oot (ignorado si dual_input=1)
seg_id     = id numerico de segmento (vacio = universo/base)
run_id     = identificador del run actual
dual_input = 0 (default) | 1

CASLIB policy:
- Input se lee desde CASLIB PROC (PATH -> data/processed/, subdirs=1).
- Output se escribe en CASLIB OUT o en un CASLIB scoped del
modulo (ej. MOD_GINI_<run_id>), siguiendo caslib_lifecycle.md.
- casuser NO se usa para datos operativos.
- CASLIBs PROC y OUT deben estar creados antes de llamar esta macro.

Convencion: el modulo debe exponer %<module>_run(...).
========================================================================= */
%macro run_module(module=, troncal_id=, split=, seg_id=, run_id=, dual_input=0);

	/* ---- Locals (no incluir paths: son %global via fw_path_processed) --- */
	%local _scope _out_caslib _promote_ok;

	/* Construir scope label para naming de outputs */
	%if %superq(seg_id)=%then %let _scope=base;
	%else %let _scope=seg%sysfunc(putn(&seg_id., z3.));

	%let _out_caslib=OUT;

	%if &dual_input.=0 %then %do;

		/* =================================================================
		MODO A: Single-input (correlacion, gini, etc.)
		================================================================= */
		%put NOTE: -----------------------------------------------------;
		%put NOTE: [run_module] module=&module. troncal=&troncal_id.
			split=&split. seg_id=&seg_id.;
		%put NOTE: -----------------------------------------------------;

		/* 1) Resolver input path (relativo al CASLIB PROC) */
		%fw_path_processed(outvar=_input_path, troncal_id=&troncal_id.,
			split=&split., seg_id=&seg_id.);

		/* 2) Promote: cargar .sashdat desde PROC y promover como _active_input */
		%_promote_castable( m_cas_sess_name=conn, m_input_caslib=PROC,
			m_subdir_data=&_input_path., m_output_caslib=PROC, m_output_data=
			_active_input );

		/* Validar promote (fail-fast) */
		%let _promote_ok=0;

		proc sql noprint;
			select count(*) into :_promote_ok trimmed from dictionary.tables
				where upcase(libname)='PROC' and upcase(memname)=
				'_ACTIVE_INPUT';
		quit;

		%if &_promote_ok.=0 %then %do;
			%put ERROR:=====================================================;
			%put ERROR: [run_module] No se pudo cargar _active_input.;
			%put ERROR: [run_module] module=&module.;
			%put ERROR: [run_module] troncal=&troncal_id.;
			%put ERROR: [run_module] split=&split.;
			%put ERROR: [run_module] seg_id=&seg_id.;
			%put ERROR: [run_module] archivo=&_input_path.;
			%put ERROR: [run_module] Verifique que el .sashdat existe en
				data/processed/.;
			%put ERROR:=====================================================;
			%return;
		%end;

		/* 3) Ejecutar modulo single-input */
		%include "&fw_root./src/modules/&module./&module._run.sas";
		%&module._run( input_caslib=PROC, input_table=_active_input,
			output_caslib=&_out_caslib., troncal_id=&troncal_id., split=&split.,
			scope=&_scope., run_id=&run_id. );

		%put NOTE: [run_module] &module. completado para
			troncal_&troncal_id./&split./&_scope.;

		/* 4) Drop tabla promovida */
		proc cas;
			session conn;
			table.dropTable / caslib="PROC" name="_active_input" quiet=true;
		quit;

	%end;
	%else %do;

		/* =================================================================
		MODO B: Dual-input (PSI, etc.) - promueve train + oot
		El parametro split= se ignora.
		================================================================= */
		%put NOTE: -----------------------------------------------------;
		%put NOTE: [run_module] module=&module. troncal=&troncal_id.
			seg_id=&seg_id. (dual_input);
		%put NOTE: -----------------------------------------------------;

		/* 1) Resolver rutas train y oot */
		%fw_path_processed(outvar=_train_path, troncal_id=&troncal_id.,
			split=train, seg_id=&seg_id.);
		%fw_path_processed(outvar=_oot_path, troncal_id=&troncal_id., split=oot,
			seg_id=&seg_id.);

		/* 2) Promote ambas tablas */
		%_promote_castable( m_cas_sess_name=conn, m_input_caslib=PROC,
			m_subdir_data=&_train_path., m_output_caslib=PROC, m_output_data=
			_train_input );
		%_promote_castable( m_cas_sess_name=conn, m_input_caslib=PROC,
			m_subdir_data=&_oot_path., m_output_caslib=PROC, m_output_data=
			_oot_input );

		/* Validar promote de ambas tablas */
		%let _promote_ok=0;

		proc sql noprint;
			select count(*) into :_promote_ok trimmed from dictionary.tables
				where upcase(libname)='PROC' and upcase(memname) in
				('_TRAIN_INPUT', '_OOT_INPUT');
		quit;

		%if &_promote_ok. < 2 %then %do;
			%put ERROR:=====================================================;
			%put ERROR: [run_module] No se pudieron cargar _train_input y/o
				_oot_input.;
			%put ERROR: [run_module] module=&module.;
			%put ERROR: [run_module] troncal=&troncal_id.;
			%put ERROR: [run_module] seg_id=&seg_id.;
			%put ERROR: [run_module] train=&_train_path.;
			%put ERROR: [run_module] oot=&_oot_path.;
			%put ERROR: [run_module] Verifique que los .sashdat existen en
				data/processed/.;
			%put ERROR:=====================================================;
			%return;
		%end;

		/* 3) Ejecutar modulo dual-input */
		%include "&fw_root./src/modules/&module./&module._run.sas";
		%&module._run( input_caslib=PROC, train_table=_train_input, oot_table=
			_oot_input, output_caslib=&_out_caslib., troncal_id=&troncal_id.,
			scope=&_scope., run_id=&run_id. );

		%put NOTE: [run_module] &module. completado para
			troncal_&troncal_id./&_scope. (dual-input).;

		/* 4) Drop ambas tablas promovidas */
		proc cas;
			session conn;
			table.dropTable / caslib="PROC" name="_train_input" quiet=true;
			table.dropTable / caslib="PROC" name="_oot_input" quiet=true;
		quit;

	%end;

%mend run_module;
