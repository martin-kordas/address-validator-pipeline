<?php

/**
 * Nastavení údaje reálná adresa
 * @author Martin Kordas
 * 
 * využívá knihovny ReactPHP pro spuštění více procesů ZMQ pro komunikaci mezi procesy
 * 
 */

use nastaveni_udaje_realna_adresa\runner\Runner;

$_SERVER["DOCUMENT_ROOT"] = dirname(__FILE__, 4);
$root = $_SERVER["DOCUMENT_ROOT"];

require_once "$root/servis/cli/nastaveni_udaje_realna_adresa/runner.php";
require_once "$root/servis/cli/nastaveni_udaje_realna_adresa/process.php";
require_once "$root/servis/cli/nastaveni_udaje_realna_adresa/model.php";
require_once "$root/servis/cli/nastaveni_udaje_realna_adresa/utils.php";

if (!request()->isCli()) presmeruj_nedostatecna_opravneni();
ob_end_flush();


$runner = new Runner;
$runner->run();
