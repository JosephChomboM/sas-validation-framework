/* =========================================================================
   fw_import_adls_to_cas.sas — Importa data .parquet desde ADLS a RAW
   Lee un archivo parquet desde Azure Data Lake Storage (ADLS) y lo
   persiste como .sashdat en la carpeta data/raw/ del proyecto.

   Flujo:
     1) Crea CASLIB RAW (PATH → data/raw/) si no existe aún
     2) Crea CASLIB temporal ADLS (lakehouse) para leer el parquet
     3) Carga parquet → CAS → guarda como .sashdat en RAW
     4) Limpia CASLIB lakehouse (el creador limpia)

   Parámetros:
     raw_path          = ruta física de data/raw (ej. &fw_root./data/raw)
     adls_storage      = account name ADLS
     adls_container    = filesystem/container ADLS
     adls_parquet_path = ruta relativa del .parquet dentro del container
     output_table      = nombre del .sashdat de salida (sin extensión)
     save_to_disk      = 1 (default) para persistir como .sashdat

   Requiere: sesión CAS "conn" activa y macros de cas_utils.sas cargadas.
   Sigue caslib_lifecycle.md: ADLS CASLIB se crea y se limpia aquí.
   ========================================================================= */

%macro fw_import_adls_to_cas(
    raw_path =,
    adls_storage =,
    adls_container =,
    adls_parquet_path =,
    output_table =,
    save_to_disk = 1
);

  %put NOTE: ======================================================;
  %put NOTE: [fw_import_adls_to_cas] INICIO;
  %put NOTE:   ADLS: &adls_storage. / &adls_container.;
  %put NOTE:   Parquet: &adls_parquet_path.;
  %put NOTE:   Output: &output_table..sashdat → RAW;
  %put NOTE: ======================================================;

  /* -----------------------------------------------------------------
     1) Crear CASLIB RAW (PATH → data/raw/) — idempotente
     ----------------------------------------------------------------- */
  %_create_caslib(
    cas_path         = &raw_path.,
    caslib_name      = RAW,
    lib_caslib       = RAW,
    global           = Y,
    cas_sess_name    = conn,
    term_global_sess = 0,
    subdirs_flg      = 0
  );

  /* -----------------------------------------------------------------
     2) Crear CASLIB temporal ADLS (lakehouse) para leer el parquet
     ----------------------------------------------------------------- */
  proc cas;
    session conn;
    table.dropcaslib / caslib="lakehouse" quiet=true;
  quit;

  caslib lakehouse datasource=(
    srctype    = "adls",
    accountname= "&adls_storage.",
    filesystem = "&adls_container."
  ) subdirs libref=casdtl;

  /* -----------------------------------------------------------------
     3) Cargar parquet desde ADLS → CAS table en CASLIB RAW
     ----------------------------------------------------------------- */
  proc casutil;
    load casdata  = "&adls_parquet_path."
         incaslib = "lakehouse"
         importoptions=(filetype="parquet")
         casout   = "&output_table."
         outcaslib= "RAW"
         replace;
  quit;

  /* Contar obs importadas */
  proc sql noprint;
    select count(*) into :_nobs_import trimmed from RAW.&output_table.;
  quit;
  %put NOTE: [fw_import_adls_to_cas] &output_table. => &_nobs_import. obs importadas;

  /* -----------------------------------------------------------------
     4) Persistir como .sashdat en data/raw/
     ----------------------------------------------------------------- */
  %if &save_to_disk. = 1 %then %do;
    proc casutil;
      save casdata  = "&output_table."
           incaslib = "RAW"
           casout   = "&output_table..sashdat"
           outcaslib= "RAW"
           replace;
    quit;
    %put NOTE: [fw_import_adls_to_cas] Guardado &output_table..sashdat en data/raw/;
  %end;

  /* -----------------------------------------------------------------
     5) Cleanup: dropear CASLIB LAKEHOUSE (el creador limpia)
     ----------------------------------------------------------------- */
  %_drop_caslib(caslib_name=lakehouse, cas_sess_name=conn, del_prom_tables=1);

  %put NOTE: [fw_import_adls_to_cas] FIN — LAKEHOUSE CASLIB limpiado.;

%mend fw_import_adls_to_cas;