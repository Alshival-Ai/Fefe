from django.urls import path

from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('team-directory/', views.team_directory, name='team_directory'),
    path('team-directory/teams/create/', views.team_directory_create_team, name='team_directory_create_team'),
    path('team-directory/teams/<int:team_id>/rename/', views.team_directory_rename_team, name='team_directory_rename_team'),
    path('team-directory/teams/<int:team_id>/delete/', views.team_directory_delete_team, name='team_directory_delete_team'),
    path('team-directory/teams/<int:team_id>/members/add/', views.team_directory_add_team_member, name='team_directory_add_team_member'),
    path('team-directory/teams/<int:team_id>/members/<int:user_id>/remove/', views.team_directory_remove_team_member, name='team_directory_remove_team_member'),
    path('team-directory/users/<int:user_id>/permissions/', views.team_directory_update_user_permissions, name='team_directory_update_user_permissions'),
    path('team-directory/users/invite/', views.team_directory_invite_user, name='team_directory_invite_user'),
    path('resources/', views.resources, name='resources'),
    path('resources/ssh-keys/add/', views.add_ssh_credential_item, name='add_ssh_credential_item'),
    path('resources/ssh-keys/<int:credential_id>/delete/', views.delete_ssh_credential_item, name='delete_ssh_credential_item'),
    path('resources/ssh-keys/global/<int:credential_id>/delete/', views.delete_global_ssh_credential_item, name='delete_global_ssh_credential_item'),
    path('resources/<int:resource_id>/', views.resource_detail, name='resource_detail'),
    path('resources/add/', views.add_resource_item, name='add_resource_item'),
    path('resources/<int:resource_id>/edit/', views.edit_resource_item, name='edit_resource_item'),
    path('resources/<int:resource_id>/delete/', views.delete_resource_item, name='delete_resource_item'),
    path('resources/<int:resource_id>/check/', views.check_resource_health, name='check_resource_health'),
]
