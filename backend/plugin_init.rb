require 'set'
class HarvardCSVModel < ASpaceExport::ExportModel
  model_for :harvard_csv

  def initialize(resource_id)
    @resource_id = resource_id.to_i
  end

  def self.from_resource_id(resource_id)
    self.new(resource_id)
  end

  CSV_HEADERS = [
        "ID",
        "Unit Title",
        "Unit Date",
        "Start Year",
        "End Year",
        "Unit ID",
        "Level",
        "Originator",
        "Container 1",
        "Container 2",
        "Container 3",
        "Phys. Desc. 1",
        "Phys. Desc. 2",
        "Phys. Desc. 3",
        "Phys. Loc",
        "Access Restrict.",
        "URN",
        "Collection Title",
        "Originator_Resource",
        "Collection Date",
        "Collection ID",
        "Parent 1",
        "Parent 2",
        "Parent 3",
        "Parent 4",
        "Parent 5",
        "Parent 6",
  ].map(&:to_sym)

  def process_row(row)
    CSV_HEADERS.map {|hdr|
      Log.debug(hdr)
      Log.debug(row)
      raw_val = row[hdr] || '' # nils are just blank
      if hdr == :'Unit Title'
        # strip HTML tags - one downside of doing this in the backend
        # is we don't have Rails sanitizers, so we turn to regexp
        raw_val.gsub!(%r{</?\w+[^>/]*/?>}, '')
      end
      raw_val.tr!("\t\r\n", " ") # col and record sep to space
      raw_val.gsub!('"', '""') # double internal quotes to escape them

      out = %Q|"#{raw_val}"|
    }.join(",")
  rescue Exception => e
    Log.error("Exception in process_row")
    if defined? hdr
      Log.error("hdr: #{hdr}")
    end
    Log.error("Error for row: #{row}")
    Log.error(e.backtrace)
    raise e
  end

  PHYSDESC_NOTE_TYPES = %w|dimensions physdesc physfacet|.to_set.freeze
  BATCH_SIZE = 100
  # Fetch AOs from a resource and present them in tree order
  #   e.g.(1, 1a, 1b, 2, 2a, 3, 4, 4a)
  def each
    DB.open do |db|
      # There are eight placeholder positions in this cursed SQL, all of which
      #  should be filled with @resource_id - MySQL doesn't support correlated subqueries
      #  so one can't reference the outer resource ID in the union within the outer query,
      #  and I don't trust it to push down a limit into the CTE either tbh.
      dataset = db[<<~SQL, *([@resource_id] * 8)]
        WITH RECURSIVE ao AS (SELECT
                  id,
                  root_record_id,
                  ref_id,
                  title,
                  component_id,
                  level_id,
                  position,
                  parent_id,
                  JSON_ARRAY(title) as parent_titles,
                  1 as depth,
                  POWER(10, 18) * (position + 1) as adjusted_pos
           FROM archival_object WHERE parent_id IS NULL AND root_record_id = ?
           UNION ALL SELECT
                  P.id,
                  P.root_record_id,
                  P.ref_id,
                  P.title,
                  P.component_id,
                  P.level_id,
                  P.position,
                  P.parent_id,
                  JSON_ARRAY_INSERT(ao.parent_titles, '$[0]', P.title) as parent_titles,
                  depth + 1 as depth,
                  (POWER(10, 20 - (2 * (ao.depth + 1))) * (P.position + 1)) + ao.adjusted_pos as adjusted_pos
           FROM archival_object P INNER JOIN ao ON P.parent_id = ao.id)
         SELECT ao.id AS ao_id,
                ao.ref_id AS "ID",
                ao.title AS "Unit Title",
                group_concat(ao_date.expression SEPARATOR ";") AS "Unit Date",
                min(ao_date.begin) AS "Start Year",
                max(ao_date.end) AS "End Year",
                ao.component_id AS "Unit ID",
                level.value AS "Level",
                group_concat(creators.sort_name SEPARATOR ';') AS "Originator",
                parent_titles->>'$[1]' as "Parent 1",
                parent_titles->>'$[2]' as "Parent 2",
                parent_titles->>'$[3]' as "Parent 3",
                parent_titles->>'$[4]' as "Parent 4",
                parent_titles->>'$[5]' as "Parent 5",
                parent_titles->>'$[6]' as "Parent 6",
                tc.indicator AS "Container 1",
                sc.indicator_2 AS "Container 2",
                sc.indicator_3 AS "Container 3",
                group_concat(fv.file_uri SEPARATOR ';') AS "URN",
                group_concat(resource_creators.sort_name SEPARATOR ';') AS "Originator_Resource",
                r.title AS "Collection Title",
                group_concat(resource_date.expression SEPARATOR '; ') AS "Collection Date",
                REGEXP_REPLACE(
                  CONCAT_WS('.',
                    JSON_VALUE(r.identifier, '$[0]'),
                    coalesce(JSON_VALUE(r.identifier, '$[1]'), ''),
                    coalesce(JSON_VALUE(r.identifier,'$[2]'), ''),
                    coalesce(JSON_VALUE(r.identifier, '$[3]'), '')
                  ),
                  '(.+[^.])\.+$', '$1') AS "Collection ID",
                note.id AS note_id,
                CONVERT(note.notes USING 'utf8mb4') AS notes
              FROM resource r
              JOIN ao ON ao.root_record_id = r.id
         LEFT JOIN enumeration_value level ON level.id = ao.level_id
         LEFT JOIN instance i ON i.archival_object_id = ao.id
         LEFT JOIN sub_container sc ON sc.instance_id = i.id
         LEFT JOIN top_container_link_rlshp tclr ON tclr.sub_container_id = sc.id
         LEFT JOIN top_container tc ON tc.id = tclr.top_container_id
         LEFT JOIN instance_do_link_rlshp idlr ON i.id = idlr.instance_id
         LEFT JOIN digital_object do ON do.id = idlr.digital_object_id
         LEFT JOIN file_version fv ON fv.digital_object_id = do.id
         LEFT JOIN (SELECT r_p.id, sort_name FROM resource r_p
                    JOIN linked_agents_rlshp r_lar_p ON r_lar_p.resource_id = r_p.id
                    JOIN agent_person r_ap ON r_lar_p.agent_person_id = r_ap.id
                    JOIN name_person r_np ON r_np.agent_person_id = r_ap.id
                    JOIN enumeration_value r_agent_role_p ON r_agent_role_p.id = r_lar_p.role_id
                    WHERE r_p.id = ?
                    AND r_agent_role_p.value IN ('creator', 'source')
                    AND r_np.is_display_name = true
                    UNION ALL
                    SELECT r_f.id, sort_name FROM resource r_f
                    JOIN linked_agents_rlshp r_lar_f ON r_lar_f.resource_id = r_f.id
                    JOIN agent_family r_af ON r_af.id = r_lar_f.agent_family_id
                    JOIN name_family r_nf ON r_nf.agent_family_id = r_af.id
                    JOIN enumeration_value r_agent_role_f ON r_agent_role_f.id = r_lar_f.role_id
                    WHERE r_f.id = ?
                    AND r_agent_role_f.value IN ('creator', 'source')
                    AND r_nf.is_display_name = true
                    UNION ALL
                    SELECT r_ce.id, sort_name FROM resource r_ce
                    JOIN linked_agents_rlshp r_lar_ce ON r_lar_ce.resource_id = r_ce.id
                    JOIN agent_corporate_entity r_ace ON r_ace.id = r_lar_ce.agent_corporate_entity_id
                    JOIN name_corporate_entity r_nce ON r_nce.agent_corporate_entity_id = r_ace.id
                    JOIN enumeration_value r_agent_role_ce ON r_agent_role_ce.id = r_lar_ce.role_id
                    WHERE r_ce.id = ?
                    AND r_agent_role_ce.value IN ('creator', 'source')
                    AND r_nce.is_display_name = true
                    ) resource_creators ON resource_creators.id = r.id
         LEFT JOIN (SELECT ao_p.id, sort_name FROM archival_object ao_p
                    JOIN linked_agents_rlshp lar ON lar.archival_object_id = ao_p.id
                    JOIN agent_person ap ON ap.id = lar.agent_person_id
                    JOIN name_person np ON np.agent_person_id = ap.id
                    JOIN enumeration_value agent_role ON agent_role.id = lar.role_id
                    WHERE ao_p.root_record_id = ?
                      AND agent_role.value IN ('creator', 'source')
                      AND np.is_display_name = true
                    UNION ALL
                    SELECT ao_f.id, sort_name FROM archival_object ao_f
                    JOIN linked_agents_rlshp larf ON larf.archival_object_id = ao_f.id
                    JOIN agent_family af ON af.id = larf.agent_family_id
                    JOIN name_family nf ON nf.agent_family_id = af.id
                    JOIN enumeration_value agent_role_f ON agent_role_f.id = larf.role_id
                    WHERE ao_f.root_record_id = ?
                      AND agent_role_f.value IN ('creator', 'source')
                      AND nf.is_display_name = true
                    UNION ALL
                    SELECT ao_ce.id, sort_name FROM archival_object ao_ce
                    JOIN linked_agents_rlshp larce ON larce.archival_object_id = ao_ce.id
                    JOIN agent_corporate_entity ace ON ace.id = larce.agent_corporate_entity_id
                    JOIN name_corporate_entity nce ON nce.agent_corporate_entity_id = ace.id
                    JOIN enumeration_value agent_role_ce ON agent_role_ce.id = larce.role_id
                    WHERE ao_ce.root_record_id = ?
                      AND agent_role_ce.value IN ('creator', 'source')
                      AND nce.is_display_name = true
                    ) creators ON creators.id = ao.id
         LEFT JOIN note ON note.archival_object_id = ao.id
         LEFT JOIN date ao_date ON ao_date.archival_object_id = ao.id
         LEFT JOIN date resource_date ON resource_date.resource_id = r.id
         WHERE r.id = ?
         GROUP BY r.id, ao_id, ao.ref_id, ao.component_id, ao.title, ao.parent_titles, ao.adjusted_pos, level, tc.indicator, sc.indicator_2, sc.indicator_3, note.id
         ORDER BY adjusted_pos ASC, ao.id ASC, note.id ASC
        SQL
      # Iterate over the dataset. Multiple rows with the same AO id are present, due to it
      #   being infeasible to group notes together, so it's necessary to collect and
      #   process multiple input_rows into one output row.
      current_id = nil # current AO id, add output row to to_dispatch when it changes
      current_csv_row = {}
      # holds CSV rows until ready to dispatch, emptied after dispatch, contains headers first
      to_dispatch = [CSV_HEADERS.map{|val|
                       case val
                       when /Originator_Resource/
                         '"Originator"'
                       else
                         %Q|"#{val.to_s}"|
                       end
                     }.join(",")]
      Log.debug('Got to dataset.each')
      dataset.each do |input_row|
        Log.debug("current_id: #{current_id}")
        Log.debug("ao_id: #{input_row[:ao_id]}")
        if current_id && current_id != input_row[:ao_id]
          Log.debug("Adding row to to_dispatch")
          to_dispatch << process_row(current_csv_row)
          if to_dispatch.length == BATCH_SIZE
            Log.debug('dispatching batch')
            yield to_dispatch.join("\r") + "\r"
            to_dispatch.clear
          end
          current_csv_row = {}
        end
        current_id = input_row[:ao_id]
        CSV_HEADERS.each do |hdr|
          Log.debug("Handling header #{hdr.inspect}")
          current_csv_row[hdr] = input_row[hdr] if input_row.has_key?(hdr)
        end

        if input_row[:note_id] && input_row[:notes]
          Log.debug("Loading note #{input_row[:note_id]} with values #{input_row[:notes]}")
          note = JSON.load(input_row[:notes])
          case note['type']
          when 'physloc'
            current_csv_row[:'Phys. Loc'] = note['content'].join(" ")

          when PHYSDESC_NOTE_TYPES
            (1..3).each do |i|
              unless current_csv_row.has_key? "Phys. Desc. #{i}".to_sym
                begin
                  content = if note.key? 'content'
                              note['content'].join(" ")
                            elsif note.key? 'subnotes'
                              note['subnotes'].map {|n|
                                n['content']
                              }.join(" ")
                            else
                              'ERROR: problem parsing note content'
                            end
                rescue StandardError => e
                  content = 'ERROR: problem parsing note content'
                end
                current_csv_row["Phys. Desc. #{i}".to_sym] = content
                break
              end
            end

          when 'accessrestrict'
            current_csv_row[:"Access Restrict."] = note['subnotes'].map {|sn| sn['content']}.join(' ')
          end
        end


        Log.debug("to_dispatch at end of processing: #{to_dispatch}")
      end # end dataset.each
      # Because we dispatch when ao_id _changes_ the last row is left over to dispatch
      to_dispatch << process_row(current_csv_row)
      Log.debug('Dispatching final batch')
      Log.debug(to_dispatch)
      yield to_dispatch.join("\r")

    end # end DB.open
    Log.debug('Closed db')
  rescue StandardError => e
    Log.error(e.backtrace.inspect)
    raise e
  end # end def each
end # end HarvardCSVModel
