# ead transform routes
ArchivesSpace::Application.routes.draw do
  match 'resources/:id/staff_csv' => 'resources_ead_xform#staff_csv_but_good', :via => [:get, :post], defaults: { format: 'csv' }
end
