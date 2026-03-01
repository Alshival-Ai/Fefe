import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Button } from '../components/ui/button.jsx';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '../components/ui/card.jsx';
import { Input } from '../components/ui/input.jsx';

const RESTART_SECONDS = 8;

function readAdminPayload() {
  const payloadNode = document.getElementById('settings-admin-data');
  if (!payloadNode) return null;
  try {
    return JSON.parse(payloadNode.textContent || '{}');
  } catch (error) {
    return null;
  }
}

function ToggleField({ name, defaultChecked, disabled, label, helpText }) {
  return (
    <label className="tw-flex tw-items-start tw-gap-3 tw-rounded-md tw-border tw-border-border tw-bg-background tw-p-3">
      <input
        className="tw-mt-1 tw-size-4 tw-accent-blue-500"
        type="checkbox"
        name={name}
        value="1"
        defaultChecked={Boolean(defaultChecked)}
        disabled={disabled}
      />
      <span className="tw-flex tw-flex-col tw-gap-1">
        <span className="tw-text-sm tw-font-medium tw-text-foreground">{label}</span>
        <span className="tw-text-xs tw-text-muted-foreground">{helpText}</span>
      </span>
    </label>
  );
}

export default function SettingsAdminApp() {
  const payload = useMemo(() => readAdminPayload(), []);
  const formRef = useRef(null);
  const actionInputRef = useRef(null);
  const intervalRef = useRef(null);
  const [countdown, setCountdown] = useState(RESTART_SECONDS);
  const [restartPending, setRestartPending] = useState(false);

  useEffect(() => {
    if (!payload) return undefined;
    const legacySection = document.querySelector('[data-settings-admin-legacy]');
    if (legacySection) legacySection.classList.add('d-none');
    return () => {
      if (legacySection) legacySection.classList.remove('d-none');
    };
  }, [payload]);

  useEffect(() => () => {
    if (intervalRef.current !== null) {
      window.clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, []);

  if (!payload) return null;

  const adminSetupReady = Boolean(payload.adminSetupReady);
  const showMicrosoftLogin = Boolean(payload.microsoftConnectorConfigured);
  const showGithubSettings = Boolean(payload.githubConnectorConfigured);

  const resetRestartState = () => {
    if (intervalRef.current !== null) {
      window.clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    setCountdown(RESTART_SECONDS);
    setRestartPending(false);
    if (actionInputRef.current) actionInputRef.current.value = 'save';
  };

  const onSaveSubmit = () => {
    if (actionInputRef.current) actionInputRef.current.value = 'save';
    resetRestartState();
  };

  const onOpenRestart = () => {
    if (intervalRef.current !== null || !adminSetupReady) return;
    if (actionInputRef.current) actionInputRef.current.value = 'save';
    setCountdown(RESTART_SECONDS);
    setRestartPending(true);
    intervalRef.current = window.setInterval(() => {
      setCountdown((current) => {
        if (current <= 1) {
          if (intervalRef.current !== null) {
            window.clearInterval(intervalRef.current);
            intervalRef.current = null;
          }
          if (actionInputRef.current) actionInputRef.current.value = 'restart_docker';
          if (formRef.current) formRef.current.submit();
          return 0;
        }
        return current - 1;
      });
    }, 1000);
  };

  return (
    <div className="tw-admin-app">
      <Card className="tw-w-full tw-bg-card/95 tw-backdrop-blur">
        <CardHeader>
          <CardTitle className="tw-text-xl">Alshival Admin</CardTitle>
          <CardDescription>Platform-wide controls for runtime behavior and maintenance operations.</CardDescription>
        </CardHeader>
        <CardContent>
          {!adminSetupReady ? (
            <div className="tw-mb-4 tw-rounded-md tw-border tw-border-amber-500/60 tw-bg-amber-500/10 tw-p-3 tw-text-sm tw-text-amber-200">
              Setup database is not ready yet. Run migrations first.
            </div>
          ) : null}
          <form
            ref={formRef}
            method="post"
            action={payload.actionUrl}
            className="tw-grid tw-gap-4"
            onSubmit={onSaveSubmit}
          >
            <input type="hidden" name="csrfmiddlewaretoken" value={payload.csrfToken} />
            <input ref={actionInputRef} type="hidden" name="admin_action" defaultValue="save" />

            <label className="tw-grid tw-gap-2">
              <span className="tw-text-sm tw-font-medium tw-text-foreground">Alshival default model</span>
              <Input
                type="text"
                name="default_model"
                defaultValue={payload.defaultModel}
                placeholder="gpt-4.1-mini"
                disabled={!adminSetupReady}
              />
              <span className="tw-text-xs tw-text-muted-foreground">Used as the platform default model selection.</span>
            </label>

            <ToggleField
              name="monitoring_enabled"
              defaultChecked={payload.monitoringEnabled}
              disabled={!adminSetupReady}
              label="Enable global resource monitoring"
              helpText="When disabled, background and manual health checks are skipped globally."
            />

            <ToggleField
              name="maintenance_mode"
              defaultChecked={payload.maintenanceMode}
              disabled={!adminSetupReady}
              label="Enable maintenance mode banner state"
              helpText="Use this to signal temporary platform maintenance operations."
            />

            <ToggleField
              name="support_inbox_monitoring_enabled"
              defaultChecked={payload.supportInboxMonitoringEnabled}
              disabled={!adminSetupReady}
              label="Enable support inbox monitoring worker"
              helpText="When disabled, the support inbox monitoring process will not poll or ingest emails."
            />

            {showMicrosoftLogin ? (
              <ToggleField
                name="microsoft_login_enabled"
                defaultChecked={payload.microsoftLoginEnabled}
                disabled={!adminSetupReady}
                label="Enable Microsoft Login"
                helpText="Displays a Sign in with Microsoft option on the login screen."
              />
            ) : null}

            {showGithubSettings ? (
              <>
                <ToggleField
                  name="github_login_enabled"
                  defaultChecked={payload.githubLoginEnabled}
                  disabled={!adminSetupReady}
                  label="Enable GitHub Login"
                  helpText="Displays a Sign in with GitHub option on the login screen."
                />
                <ToggleField
                  name="ask_github_mcp_enabled"
                  defaultChecked={payload.askGithubMcpEnabled}
                  disabled={!adminSetupReady}
                  label="Enable GitHub MCP In Ask Alshival"
                  helpText="Adds GitHub MCP tools to Ask Alshival alongside internal tools."
                />
              </>
            ) : null}

            <ToggleField
              name="ask_asana_mcp_enabled"
              defaultChecked={payload.askAsanaMcpEnabled}
              disabled={!adminSetupReady}
              label="Enable Asana MCP In Ask Alshival"
              helpText="Requires Asana MCP OAuth credentials/token via env (ASK_ASANA_MCP_*)."
            />

            <label className="tw-grid tw-gap-2">
              <span className="tw-text-sm tw-font-medium tw-text-foreground">Maintenance message</span>
              <Input
                type="text"
                name="maintenance_message"
                defaultValue={payload.maintenanceMessage}
                maxLength={255}
                placeholder="Scheduled maintenance in progress. Monitoring is temporarily paused."
                disabled={!adminSetupReady}
              />
            </label>

            <CardFooter className="tw-flex tw-flex-wrap tw-gap-3 tw-px-0 tw-pb-0">
              <Button type="submit" disabled={!adminSetupReady}>Save admin settings</Button>
              <Button
                type="button"
                variant="outline"
                onClick={onOpenRestart}
                disabled={!adminSetupReady || restartPending}
              >
                Restart Docker App
              </Button>
            </CardFooter>

            {restartPending ? (
              <div className="tw-rounded-md tw-border tw-border-amber-500/60 tw-bg-amber-500/10 tw-p-3 tw-text-sm tw-text-amber-200">
                <p className="tw-m-0">
                  Restart will begin in <strong>{countdown}</strong> seconds. It may take a few minutes for the application to come back online.
                </p>
                <div className="tw-mt-3">
                  <Button type="button" variant="outline" size="sm" onClick={resetRestartState}>
                    Cancel restart
                  </Button>
                </div>
              </div>
            ) : null}
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
