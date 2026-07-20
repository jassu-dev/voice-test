declare namespace NodeJS {
  export interface ProcessEnv {
    API_URL: string;
    XAI_API_KEY: string;
    HOSTNAME: string;
    PORT?: string;
    TWILIO_ACCOUNT_SID?: string;
    TWILIO_AUTH_TOKEN?: string;
    TWILIO_PHONE_NUMBER?: string;
    TARGET_PHONE_NUMBER?: string;
  }
}
