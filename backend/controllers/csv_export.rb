# frozen_string_literal: true

class ArchivesSpaceService < Sinatra::Base
  include ExportHelpers

  Endpoint.get('/repositories/:repo_id/resource_descriptions/:id.csv')
    .description("Get a CSV describing contents of a resource")
    .example("shell") do
      <<~SHELL
       curl -s -F password="admin" "http://localhost:8089/users/admin/login"
        set SESSION="session_id"
        curl -H "X-ArchivesSpace-Session: $SESSION" \\
        "http://localhost:8089/repositories/2/resource_descriptions/577.csv" //
        --output resource_577.csv
      SHELL
    end
    .example("python") do
      <<~PYTHON
        from asnake.client import ASnakeClient  # import the ArchivesSnake client

        client = ASnakeClient(baseurl="http://localhost:8089", username="admin", password="admin")
        # replace http://localhost:8089 with your ArchivesSpace API URL and admin for your username and password

        client.authorize()  # authorizes the client

        my_csv = client.get("repositories/2/resource_descriptions/577.csv")
        # replace 2 for your repository ID and 577 with your resource ID. Find these at the URI on the staff interface
        # set parameters to True or False

        with open("resource_577.csv", "wb") as file:  # save the file
            file.write(my_csv.content)  # write the file content to our file.
            file.close()

        # For error handling, print or log the returned value of client.get with .json() - print(my_csv.json())
      PYTHON
    end
    .params(["id", :id],
            ["repo_id", :repo_id])
    .permissions([:view_repository])
    .returns([200, "CSV describing resource"]) \
  do
    attachment "resource_#{params[:id]}.csv"
    csv = HarvardCSVModel.new(params[:id])
    stream_response(csv, 'text/csv')
  end
end
