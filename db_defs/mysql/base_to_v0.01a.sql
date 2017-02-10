use panopticon;

drop view current_package_versions;
drop view host_package_versions;

CREATE TABLE current_package_versions (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	package_id INT UNSIGNED NOT NULL UNIQUE,
	package_history_id INT UNSIGNED NOT NULL UNIQUE,
	CONSTRAINT cpv_id_primary_key PRIMARY KEY USING BTREE(id),
	CONSTRAINT FOREIGN KEY cpv_package_id_foreign_key (package_id) REFERENCES package(id) ON DELETE RESTRICT ON UPDATE CASCADE,
	CONSTRAINT FOREIGN KEY cpv_package_history_id_foreign_key (package_history_id) REFERENCES package_history(id) ON DELETE CASCADE ON UPDATE CASCADE);

CREATE TABLE host_package_versions (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	host_id INT UNSIGNED NOT NULL,
	package_id INT UNSIGNED NOT NULL,
	package_history_id INT UNSIGNED NOT NULL,
	CONSTRAINT UNIQUE KEY hpv_host_id_package_id_unique_key USING BTREE (host_id, package_id),
	CONSTRAINT hpv_id_primary_key PRIMARY KEY USING BTREE(id),
	CONSTRAINT FOREIGN KEY hpv_host_id_foreign_key (host_id) REFERENCES host(id) ON DELETE RESTRICT ON UPDATE CASCADE,
	CONSTRAINT FOREIGN KEY hpv_package_id_foreign_key (package_id) REFERENCES package(id) ON DELETE RESTRICT ON UPDATE CASCADE,
	CONSTRAINT FOREIGN KEY hpv_package_history_id_foreign_key (package_history_id) REFERENCES package_history(id) ON DELETE CASCADE ON UPDATE CASCADE);

INSERT INTO current_package_versions (package_id, package_history_id)
SELECT p.id AS package_id, ph.id AS package_history_id FROM package AS p LEFT JOIN
	(SELECT MAX(id) AS id, package_id FROM package_history GROUP BY package_id) AS ph
ON p.id = ph.package_id;

INSERT INTO host_package_versions (host_id, package_id, package_history_id)
SELECT h.id, p.id, ph.id
FROM 
	(SELECT MAX(id) AS id, package_id FROM host_update_history GROUP BY package_id) AS temp1
LEFT JOIN host_update_history AS huh ON temp1.id = huh.id
LEFT JOIN host AS h ON h.id = huh.host_id
LEFT JOIN package_history AS ph ON ph.id = huh.package_history_id
LEFT JOIN package AS p ON p.id = huh.package_id;
