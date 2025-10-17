package com.nhswd.kafka.custom.http.connector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GitHubEvent {
    @JsonProperty("type")
    private String type;

    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("repo")
    private Repo repo;

    // Getters and Setters
    public String getType() {return type; }
    public void setType(String type) {this.type = type;}

    public String getCreatedAt() { return createdAt; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }

    public Repo getRepo() { return repo; }
    public void setRepo(Repo repo) { this.repo = repo; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Repo {
        @JsonProperty("name")
        private String name;

        // Getter and Setter
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }
}