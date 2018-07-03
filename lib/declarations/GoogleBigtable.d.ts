declare module "@google-cloud/bigtable" {

    namespace Bigtable {

        export interface Table {
            insert: (...args: any[]) => any;
            createReadStream: (...args: any[]) => any;
            exists: (...args: any[]) => any;
            create: (...args: any[]) => any;
            family: (...args: any[]) => any;
            row: (...args: any[]) => any;
            delete: (...args: any[]) => any;
        }

        export interface Instance {
            table: (...args: any[]) => any;
            exists: (...args: any[]) => any;
            create: (...args: any[]) => any;
        }
    }

    class Bigtable {
        constructor(config: any);
        public table: (...args: any[]) => any;
        public instance: (...args: any[]) => any;
    }

    export = Bigtable;
}
